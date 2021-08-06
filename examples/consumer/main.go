package main

import (
	"context"
	"log"
	"time"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/diegodesousas/go-rabbitmq/consumer"
)

type MessageBody struct {
	ProcessingTime  Duration `json:"processing_time"`
	ExternalService bool     `json:"external_service"`
}

var ErrProcessingIsToLong = consumer.NewError("processing message is too long", false)
var ErrExternalServiceUnavailable = consumer.NewError("external service unavailable", true)

func HelloHandler(ctx context.Context, message consumer.Message) *consumer.Error {
	var bodyMessage MessageBody

	err := message.Unmarshal(&bodyMessage)
	if err != nil {
		return consumer.WrapErrConsumer(err)
	}

	if bodyMessage.ProcessingTime.Duration > 5*time.Second {
		return ErrProcessingIsToLong
	}

	if bodyMessage.ExternalService {
		return ErrExternalServiceUnavailable
	}

	log.Printf("Received a message")
	time.Sleep(bodyMessage.ProcessingTime.Duration)
	log.Printf("Finish processing: %v;", bodyMessage)

	return nil
}

func main() {
	log.Println("starting consumer")

	conn, err := connection.Dial(connection.Config{
		Username: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     "5672",
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	helloWorldConsumer, err := consumer.New(
		consumer.WithConnection(conn),
		consumer.WithQueue("hello.world"),
		consumer.WithQtyRoutines(5),
		consumer.WithHandler(HelloHandler),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	err = helloWorldConsumer.Consume(ctx)
	if err != nil {
		log.Fatal(err)
	}

	forever := make(chan bool)
	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
