package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/juju/loggo"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v2"
)

type Config struct {
	KafkaBroker string `yaml:"kafkaBroker"`
	MongoUri    string `yaml:"mongoUri"`
	Database    string `yaml:"database"`
	// By limiting ourselves to these collections I can assume that a
	// topic already exists and has messages.  If that assumption
	// changes, we'll need to check if a topic exists, be able to
	// create if it doesn't and then start the mongo changestream
	// watcher from the start of that collection.
	Collections []string `yaml:"collections"`
}

var logger = loggo.GetLogger("main")

func main() {
	logger.SetLogLevel(loggo.INFO)
	logger.Infof("Started")
	dat, err := ioutil.ReadFile("config.yml")
	if err != nil {
		logger.Errorf(err.Error())
		os.Exit(1)
	}
	config := Config{}
	err = yaml.Unmarshal(dat, &config)
	if err != nil {
		logger.Errorf(err.Error())
		os.Exit(1)
	}
	db, err := openDatabase(config)
	if err != nil {
		logger.Errorf(err.Error())
		os.Exit(1)
	}

	expectedCollections := make(map[string]bool)
	for i := 0; i < len(config.Collections); i++ {
		expectedCollections[config.Collections[i]] = true
	}

	collections := make(map[string]*mongo.Collection)
	channel := make(chan string)

	for {
		logger.Debugf("Listing all collections")
		err = startWatchingNewCollections(db, channel, expectedCollections, collections, config.KafkaBroker)
		if err != nil {
			// TODO: probably want to die if we error enough times
			logger.Errorf(err.Error())
		}
		sleepAndCleanup(channel, collections)
	}
}

func openDatabase(config Config) (*mongo.Database, error) {
	client, err := mongo.NewClient(config.MongoUri)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	db := client.Database(config.Database)
	return db, nil
}

func startWatchingNewCollections(db *mongo.Database, c chan string, expectedCollections map[string]bool, collections map[string]*mongo.Collection, broker string) error {
	cursor, err := db.ListCollections(context.Background(), bson.D{})
	if err != nil {
		return err
	}
	for cursor.Next(context.Background()) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			logger.Errorf(err.Error())
			continue
		}
		collectionName := result["name"].(string)
		_, ok := expectedCollections[collectionName]
		if !ok {
			logger.Infof("Collection %s is unexpected", collectionName)
			continue
		}
		_, ok = collections[collectionName]
		if ok {
			continue
		}
		collection := db.Collection(collectionName)
		collections[collectionName] = collection
		go WatchCollection(broker, collection, c)
	}
	return nil
}

func sleepAndCleanup(channel chan string, collections map[string]*mongo.Collection) {
	for {
		shouldBreak := false
		select {
		case doneCollection := <-channel:
			// We've stopped watching this collection so it needs
			// to be removed from the collections map so that
			// on the next pass through we can re-add it
			delete(collections, doneCollection)
		case <-time.After(15 * time.Second):
			shouldBreak = true
		}
		if shouldBreak {
			break
		}
	}
}

// Opens up a changestream cursor on the collection and writes new
// documents to the kafka broker.
// If there is an error, we send a message to the channel indicating
// that we've stopped watching
func WatchCollection(broker string, collection *mongo.Collection, c chan string) {
	defer func() { logger.Infof("Stopping watcher for %s.%s", collection.Database().Name(), collection.Name()) }()
	// The channel is used to indicate that an error has happened
	// watching the collection.  Hopefully the main goroutine will be
	// able to restart us.
	defer func() { c <- collection.Name() }()
	logger.Infof("Watching %s.%s", collection.Database().Name(), collection.Name())

	cs := options.ChangeStream()
	topic := fmt.Sprintf("mongo_%s_%s", collection.Database().Name(), collection.Name())
	lastMessage, err := getLastMessage(broker, topic)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	payload := lastMessage["payload"].(map[string]interface{})
	token, ok := payload["resumeToken"]
	if ok {
		logger.Debugf("Using resumeToken")
		cs.SetResumeAfter(bson.M{"_data": token})
	} else {
		logger.Debugf("Using timestamp")
		timestamp := uint32(payload["timestamp"].(float64))
		inc := uint32(payload["order"].(float64))
		// inc is a counter so its safe to just increment one to get the next document.
		// If we don't increment one, we get the same document that was already in kafka.
		// https://docs.mongodb.com/manual/reference/bson-types/#timestamps
		cs.SetStartAtOperationTime(&primitive.Timestamp{timestamp, inc + 1})
	}
	cursor, err := collection.Watch(context.Background(), mongo.Pipeline{}, cs)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	logger.Debugf("Waiting for documents on: %s", collection.Name())
	for cursor.Next(context.Background()) {
		var item bson.M
		cursor.Decode(&item)
		operationType := item["operationType"].(string)
		if operationType != "insert" {
			logger.Warningf("Document has operationType %s, expected insert", operationType)
			continue
		}
		// Note that this needs to be synchronous. If this was
		// asynchronous and something goes wrong it might be possible
		// for event B to get into kafka and not event A and so event
		// A would be lost forever
		err = SendToKafka(w, item)
		if err != nil {
			logger.Errorf(err.Error())
			return
		}
	}
}

// Returns the last message on kafka for the given topic in partition 0.
// This probably only works correctly if there is only one partition
func getLastMessage(broker string, topic string) (map[string]interface{}, error) {
	logger.Debugf(topic)
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	_, last, err := conn.ReadOffsets()
	// Would be nice if I could re-use the connection from above
	// but that is not part of the library
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  0,
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(last - 1)
	m, err := r.ReadMessage(context.Background())
	if err != nil {
		logger.Errorf(err.Error())
		return nil, err
	}
	var f interface{}
	err = json.Unmarshal(m.Value, &f)
	return f.(map[string]interface{}), nil
}

// Converts and serializes the input document and then
// sends it along to kafka.
func SendToKafka(w *kafka.Writer, doc bson.M) error {
	msgValue, err := ConvertToOldFormat(doc)
	if err != nil {
		return err
	}
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

func ConvertToOldFormat(doc bson.M) (connectSchema, error) {
	namespace := doc["ns"].(bson.M)
	name := fmt.Sprintf("mongodbschema_%s_%s", namespace["db"], namespace["coll"])
	timestamp := doc["clusterTime"].(primitive.Timestamp)
	fullDocument := doc["fullDocument"].(bson.M)
	// This transformation is to remain compatible with the previous
	// oplog reader
	fullDocument["_id"] = bson.M{"$oid": fullDocument["_id"]}
	documentBytes, err := json.Marshal(fullDocument)
	if err != nil {
		logger.Errorf(err.Error())
		return connectSchema{}, err
	}
	resumeToken := doc["_id"].(bson.M)["_data"].(string)
	logger.Debugf(resumeToken)
	// The whole connectSchema will also be json encoded
	// and so we need convert the bytes into a string
	// otherwise the []bytes get encoded using base64
	documentStr := string(documentBytes)
	results := connectSchema{
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
	return results, nil
}
