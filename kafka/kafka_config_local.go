package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "kafka-go"
	broker1Address = "localhost:9092"
)

func StartKafkaLocal() {
	i := 0

	l := log.New(os.Stdout, "kafka reader: ", 0)

	// w := kafka.NewWriter(kafka.WriterConfig{
	// 	Brokers: []string{broker1Address},
	// 	Topic:   topic,
	// })

	w := &kafka.Writer{
		Addr:         kafka.TCP(broker1Address),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Completion: func(messages []kafka.Message, err error) {
			fmt.Println("Completed")
		},
		Logger: l,
	}
	defer w.Close()
	for {
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		err := w.WriteMessages(context.Background(), kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: []byte("this is message " + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		fmt.Println("writes:", i)
		i++
		time.Sleep(time.Second)
	}
}
