package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type message struct {
	MessageNumber int    `json:"msg_num"`
	IP            string `json:"ip"`
	CollectedAt   int64  `json:"collected_at"`
	User          string `json:"user"`
	RandomKey     string `json:"rand_key"`
}

var msgsNum = flag.Int("msgs", 0, "number of messages that will be produced to Kafka")

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	kafkaBroker = "127.0.0.1:9092"
	kafkaTopic  = "go-scala-avro"
	ipMask      = "127.0.0.%d"
	schema      = `
	{
	  "type": "record",
	  "name": "message",
	  "doc:": "A basic schema for storing message data",
	  "namespace": "com.goscalaavro",
	  "fields": [
		{ "doc": "Message number", "type": "int", "name": "msg_num" },
		{ "doc": "User IP", "type": "string", "name": "ip" },
		{ "doc": "Date of collect", "type": "long", "name": "collected_at" },
		{ "doc": "User name", "type": "string", "name": "user" },
		{ "doc": "Random key", "type": "string", "name": "rand_key" }
	  ]
	}`
)

func init() {
	flag.Parse()

}

func main() {
	msgs := make(chan *message, *msgsNum)
	done := make(chan bool)

	go buildMessages(msgs)
	go produceMessages(msgs, done)

	<-done
}

func buildMessages(msgs chan<- *message) {
	for i := 0; i < *msgsNum; i++ {
		msgs <- &message{
			MessageNumber: i,
			IP:            fmt.Sprintf(ipMask, i),
			CollectedAt:   time.Now().UTC().Unix(),
			User:          randString(10),
			RandomKey:     randString(20),
		}
	}
	close(msgs)
}

func produceMessages(msgs <-chan *message, done chan<- bool) {
	for m := range msgs {
		sendToKafka(m)
	}
	done <- true
}

func sendToKafka(msg *message) {
	c := sarama.NewConfig()
	c.Producer.Retry.Max = 3
	c.Producer.Return.Successes = true

	now := strconv.Itoa(int(time.Now().Unix()))
	pm := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key:   sarama.StringEncoder(now),
		Value: sarama.StringEncoder(avroEncodedMessage(msg)),
	}

	producer, err := sarama.NewAsyncProducer([]string{kafkaBroker}, c)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	producer.Input() <- pm

ProducerLoop:
	for {
		select {
		case err := <-producer.Errors():
			panic(err)
		case s := <-producer.Successes():
			fmt.Printf("Success producing message %d to topic %s\n", msg.MessageNumber, s.Topic)
			break ProducerLoop
		}
	}
}

func avroEncodedMessage(msg *message) []byte {
	rec, err := goavro.NewCodec(schema)
	if err != nil {
		panic(err)
	}

	b, err := rec.BinaryFromNative(nil, msg.getMap())
	if err != nil {
		panic(err)
	}
	return b
}

func randString(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (msg *message) getMap() map[string]interface{} {
	var gen interface{}
	b, _ := json.Marshal(msg)
	json.Unmarshal(b, &gen)
	return gen.(map[string]interface{})
}
