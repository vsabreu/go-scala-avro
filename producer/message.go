package main

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/linkedin/goavro"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Message represents a message for Kafka
type Message struct {
	ID          int    `json:"id"`
	CollectedAt int64  `json:"collected_at"`
	User        string `json:"user"`
	Key         string `json:"key"`
}

// NewMessage returns a new Message
func NewMessage(i int) *Message {
	return &Message{
		ID:          i,
		CollectedAt: time.Now().UTC().Unix(),
		User:        randString(10),
		Key:         randString(20),
	}
}

// ToAvro encodes a Message in Avro byte array
func (msg *Message) ToAvro() []byte {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		panic(err)
	}

	b, err := c.BinaryFromNative(nil, msg.toMap())
	if err != nil {
		panic(err)
	}
	return b
}

// toMap transforms a Message struct in a map[string]interface{}
func (msg *Message) toMap() map[string]interface{} {
	var m interface{}

	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	json.Unmarshal(b, &m)
	return m.(map[string]interface{})
}

func randString(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
