package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	avro "github.com/elodina/go-avro"
	schemaReg "github.com/elodina/go-kafka-avro"
)

var (
	kafkaBroker = []string{"127.0.0.1:9092"}
	msgsNum     = flag.Int("msgs", 0, "number of messages that will be produced to Kafka")
)

const (
	kafkaTopic        = "go-scala-avro-topic"
	schemaRegistryURL = "http://127.0.0.1:8081"
	schemaName        = "message"
	schema            = `{"type":"record","name":"message","doc:":"A basic schema for storing message data",
		"namespace":"com.goscalaavro","fields":[{"doc":"Message number","type":"int","name":"id"},
		{"doc":"Date of collect","type":"long","name":"collected_at"},
		{"doc":"User name","type":"string","name":"user"},{"doc":"Random key","type":"string","name":"key"}]}`
)

func init() {
	flag.Parse()
	registerSchema()
}

func main() {
	msgs := make(chan *Message, *msgsNum)
	done := make(chan bool)

	go buildMessages(msgs)
	go produceMessages(msgs, done)

	<-done
}

func buildMessages(msgs chan<- *Message) {
	for i := 0; i < *msgsNum; i++ {
		msgs <- NewMessage(i)
	}
	close(msgs)
}

func produceMessages(msgs <-chan *Message, done chan<- bool) {
	for m := range msgs {
		sendToKafka(m)
	}
	done <- true
}

func sendToKafka(msg *Message) {
	c := sarama.NewConfig()
	c.Producer.Retry.Max = 3
	c.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(kafkaBroker, c)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	now := strconv.Itoa(int(time.Now().Unix()))
	producer.Input() <- &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key:   sarama.StringEncoder(now),
		Value: sarama.StringEncoder(msg.ToAvro()),
	}

ProducerLoop:
	for {
		select {
		case err := <-producer.Errors():
			panic(err)
		case s := <-producer.Successes():
			fmt.Printf("Success producing message %d to topic %s\n", msg.ID, s.Topic)
			break ProducerLoop

		}
	}
}

func registerSchema() {
	s, err := avro.ParseSchema(schema)
	if err != nil {
		panic(err)
	}

	c := schemaReg.NewCachedSchemaRegistryClient(schemaRegistryURL)
	id, err := c.Register(schemaName, s)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Registered schema \"%s\" with id %d\n", schemaName, id)
}
