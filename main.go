package main

import (
	"fmt"
	appKafka "kafkaProducer/kafka"
	"time"
)

func main() {
	topic := "kafka-go"
	clientID := "g1"

	fmt.Println("Producer is being started!")
	defer fmt.Println("Producer is stopped!")

	appKafka.StartKafka(topic, clientID)
	runtime := 10 * time.Minute
	fmt.Println("Will be running for next ", runtime)
	time.Sleep(runtime)
}
