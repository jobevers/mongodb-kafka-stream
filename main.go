package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/segmentio/kafka-go"
)

// By limiting ourselves to these collections I can assume that a
// topic already exists and has messages.  If that assumption changes,
// we'll need to check if a topic exists, be able to create if it
// doesn't and then start the mongo changestream watcher from the
// start of that collection.
var expectedCollections = map[string](bool){
	"rf":      true,
	"adt":     true,
	"lab":     true,
	"ecg":     true,
	"vf":      true,
	"labMuse": true,
	"nf":      true,
}

func main() {
	fmt.Println("hello world")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	client, err := mongo.NewClient("mongodb://localhost:27017/?replicaSet=rs0")
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	db := client.Database("mirth")

	collections := make(map[string]*mongo.Collection)

	channel := make(chan string)

	for {
		fmt.Println("Listing all collections")
		cursor, err := db.ListCollections(context.Background(), bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("we got here 1")
		for cursor.Next(context.Background()) {
			fmt.Println("we got here 2")
			var result bson.M
			err := cursor.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			collectionName := result["name"].(string)
			_, ok := expectedCollections[collectionName]
			if !ok {
				log.Printf("Collection %s is unexpected", collectionName)
				continue
			}
			// TODO: if a watcher dies, it needs to be removed from collections
			_, ok = collections[collectionName]
			if ok {
				continue
			}
			collection := db.Collection(collectionName)
			collections[collectionName] = collection
			go WatchCollection(collection, channel)
		}
		fmt.Println("we got here 3")
		for {
			shouldBreak := false
			fmt.Println("we got here 3.1")
			select {
			case doneCollection := <-channel:
				// We've stopped watching this collection so it needs
				// to be removed from the collections map so that
				// on the next pass through we can re-add it
				delete(collections, doneCollection)
				fmt.Println("we got here 3.2")
			case <-time.After(15 * time.Second):
				fmt.Println("we got here 3.3")
				shouldBreak = true
			}
			if shouldBreak {
				break
			}
		}
	}
}

func getLastMessage(topic string) map[string]interface{} {
	fmt.Println(topic)
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	first, last, err := conn.ReadOffsets()
	fmt.Println(first)
	fmt.Println(last)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Partition: 0,
		MinBytes:  0,
		MaxBytes:  10e6, // 10MB
	})
	fmt.Println("we got here 4")
	r.SetOffset(last - 1)
	m, err := r.ReadMessage(context.Background())
	fmt.Println("we got here 5")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("we got here 6")
	var f interface{}
	err = json.Unmarshal(m.Value, &f)
	return f.(map[string]interface{})
}

func WatchCollection(collection *mongo.Collection, c chan string) {
	// The channel is used to indicate that an error has happened
	// watching the collection.  Hopefully the main goroutine will be
	// able to restart us.
	defer func() { c <- collection.Name() }()
	fmt.Printf("Watching %s\n", collection.Name())

	cs := options.ChangeStream()
	topic := fmt.Sprintf("mongo_%s_%s", collection.Database().Name(), collection.Name())
	lastMessage := getLastMessage(topic)
	payload := lastMessage["payload"].(map[string]interface{})
	token, ok := payload["resumeToken"]
	if ok {
		fmt.Println("Using resumeToken")
		cs.SetResumeAfter(bson.M{"_data": token})
	} else {
		fmt.Println("Using timestamp")
		timestamp := uint32(payload["timestamp"].(float64))
		inc := uint32(payload["order"].(float64))
		// inc is a counter so its safe to just increment one to get the next document.
		// If we don't increment one, we get the same document that was already in kafka.
		// https://docs.mongodb.com/manual/reference/bson-types/#timestamps
		cs.SetStartAtOperationTime(&primitive.Timestamp{timestamp, inc + 1})
	}
	cursor, err := collection.Watch(context.Background(), mongo.Pipeline{}, cs)
	if err != nil {
		log.Println(err)
		return
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	fmt.Printf("Waiting for documents on: %s\n", collection.Name())
	for cursor.Next(context.Background()) {
		fmt.Printf(".")
		var item bson.M
		cursor.Decode(&item)
		operationType := item["operationType"].(string)
		if operationType != "insert" {
			log.Printf("Warning, document has operationType %s, expected insert", operationType)
			continue
		}
		// Note that this needs to be synchronous. If this was
		// asynchronous and something goes wrong it might be possible
		// for event B to get into kafka and not event A and so event
		// A would be lost forever
		err = SendToKafka(w, item)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func SendToKafka(w *kafka.Writer, doc bson.M) error {
	msgValue := ConvertToOldFormat(doc)
	output, err := json.Marshal(msgValue)
	if err != nil {
		return err
	}
	msg := kafka.Message{Value: output}
	w.WriteMessages(context.Background(), msg)
	return nil
}

type connectSchema struct {
	Schema  payloadSchema `json:"schema"`
	Payload payloadData   `json:"payload"`
}

type payloadSchema struct {
	Type     string  `json:"type"`
	Optional bool    `json:"optional"`
	Fields   []field `json:"fields"`
	Name     string  `json:"name"`
}

type field struct {
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
	Field    string `json:"field"`
}

type payloadData struct {
	Timestamp   uint32 `json:"timestamp"`
	Order       uint32 `json:"order"`
	Operation   string `json:"operation"`
	Database    string `json:"database"`
	Object      string `json:"object"`
	ResumeToken string `json:"resumeToken"`
}

func ConvertToOldFormat(doc bson.M) connectSchema {
	namespace := doc["ns"].(bson.M)
	name := fmt.Sprintf("mongodbschema_%s_%s", namespace["db"], namespace["coll"])
	timestamp := doc["clusterTime"].(primitive.Timestamp)
	fullDocument := doc["fullDocument"].(bson.M)
	// This tranformation is to remain compatible with the previous
	// oplog reader
	fullDocument["_id"] = bson.M{"$oid": fullDocument["_id"]}
	documentBytes, err := json.Marshal(fullDocument)
	if err != nil {
		log.Fatal(err)
	}
	resumeToken := doc["_id"].(bson.M)["_data"].(string)
	fmt.Println(resumeToken)
	// The whole connectSchema will also be json encoded
	// and so we need convert the bytes into a string
	// otherwise the []bytes get encoded using base64
	documentStr := string(documentBytes)
	return connectSchema{
		Schema: payloadSchema{
			Type:     "struct",
			Optional: false,
			Name:     name,
			Fields: []field{
				field{"int32", true, "timestamp"},
				field{"int32", true, "order"},
				field{"string", true, "operation"},
				field{"string", true, "database"},
				field{"string", true, "object"},
				field{"string", true, "resumeToken"}}},
		Payload: payloadData{
			Timestamp:   timestamp.T,
			Order:       timestamp.I,
			Operation:   "i",
			Database:    fmt.Sprintf("%s.%s", namespace["db"], namespace["coll"]),
			Object:      documentStr,
			ResumeToken: resumeToken}}
}
