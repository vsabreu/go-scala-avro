package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

var (
	kafkaTopic  = "go-scala-avro"
	kafkaBroker = []string{"127.0.0.1:9092"}
	msgsNum     = flag.Int("msgs", 0, "number of messages that will be produced to Kafka")
)

func init() {
	flag.Parse()
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

ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{
			Topic: kafkaTopic,
			Key:   sarama.StringEncoder(now),
			Value: sarama.StringEncoder(msg.ToAvro()),
		}:

		case err := <-producer.Errors():
			panic(err)
		case s := <-producer.Successes():
			fmt.Printf("Success producing message %d to topic %s\n", msg.ID, s.Topic)
			break ProducerLoop

		}
	}
}
