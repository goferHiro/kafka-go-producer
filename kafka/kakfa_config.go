package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func newDialer(clientID, username, password string) *kafka.Dialer {
	mechanism := plain.Mechanism{
		Username: username,
		Password: password,
	}

	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		ClientID:      clientID,
		SASLMechanism: mechanism,
		TLS: &tls.Config{
			InsecureSkipVerify: false,
			RootCAs:            rootCAs,
		},
	}
}

func createTopic(url string, topic string, dialer *kafka.Dialer) {
	conn, err := dialer.Dial("tcp", url)
	try(err, nil)
	defer conn.Close()

	controller, err := conn.Controller()
	try(err, nil)

	controllerConn, err := dialer.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	try(err, nil)
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     6,
			ReplicationFactor: 3,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	try(err, fmt.Println) //topic probably exists

}

func newWriter(url string, topic string, dialer *kafka.Dialer) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{url},
		Topic:        topic,
		Balancer:     &kafka.CRC32Balancer{},
		Dialer:       dialer,
		BatchSize:    10,
		BatchTimeout: 1 * time.Millisecond,
	})
}

func write(url string, topic string, dialer *kafka.Dialer) {
	writer := newWriter(url, topic, dialer)
	defer writer.Close()
	for i := 0; ; i++ {
		v := []byte("V" + strconv.Itoa(i))
		log.Printf("send:\t%s\n", v)
		msg := kafka.Message{Key: v, Value: v}
		err := writer.WriteMessages(context.Background(), msg)
		try(err, nil)
		time.Sleep(100 * time.Millisecond)
	}
}

func StartKafka(topic, clientID string) {
	url :=os.Getenv("bootstrap_servers")

	username := os.Getenv("username")
	password := os.Getenv("pass")
	dialer := newDialer(clientID, username, password)
	ctx := context.Background()
	// createTopic(url, topic, dialer)
	go write(url, topic, dialer)
	<-ctx.Done()
}

func main() {
	topic := os.Getenv("topic")
	clientID := "g1"
	StartKafka(topic, clientID)
}

func try(err error, errorHandler func(...interface{}) (int, error)) {
	if err == nil {
		return
	}
	if errorHandler == nil {
		panic(err.Error())
	}
	errorHandler(string(err.Error()))
}
