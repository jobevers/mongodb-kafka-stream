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

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test",
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

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

	for {
		fmt.Println("Listing all collections")
		cursor, err := db.ListCollections(context.Background(), bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		for cursor.Next(context.Background()) {
			var result bson.M
			err := cursor.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			collectionName := result["name"].(string)
			_, ok := collections[collectionName]
			if ok {
				time.Sleep(5 * time.Second)
				continue
			}
			collection := db.Collection(collectionName)
			collections[collectionName] = collection
			go WatchCollection(w, collection)
		}
		fmt.Println("we got here")
		time.Sleep(15 * time.Second)
	}
}

func WatchCollection(w *kafka.Writer, collection *mongo.Collection) {
	fmt.Printf("Watching %s\n", collection.Name())
	// TODO: pass in ChangeStreamOptions so that I can actually resume somewhere
	//       instead of just at the last one
	cs := options.ChangeStream()
	cs.SetStartAtOperationTime(&primitive.Timestamp{uint32(time.Now().Unix()), 0})
	cursor, err := collection.Watch(context.Background(), mongo.Pipeline{}, cs)
	if err != nil {
		// TODO: should just exit and let the main program restart the watcher
		log.Fatal(err)
	}
	fmt.Printf("Waiting for documents on: %s\n", collection.Name())
	for cursor.Next(context.Background()) {
		fmt.Printf(".")
		var item bson.M
		cursor.Decode(&item)
		SendToKafka(w, item)
	}
}

func SendToKafka(w *kafka.Writer, doc bson.M) {
	msgValue := ConvertToOldFormat(doc)
	output, err := json.Marshal(msgValue)
	if err != nil {
		// TODO: should just exit and let the main program restart the watcher
		log.Fatal(err)
	}
	msg := kafka.Message{Value: output}
	w.WriteMessages(context.Background(), msg)
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
			Timestamp: timestamp.T,
			Order:     timestamp.I,
			Operation: "i",
			Database:  fmt.Sprintf("%s.%s", namespace["db"], namespace["coll"]),
			Object:    documentStr,
			ResumeToken: resumeToken}}
}
