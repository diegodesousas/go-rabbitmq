package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/diegodesousas/go-rabbitmq/consumer"
	"github.com/streadway/amqp"
)

type MessageBody struct {
	ProcessingTime  Duration `json:"processing_time"`
	ExternalService bool     `json:"external_service"`
}

var ErrProcessingIsToLong = consumer.NewErrConsumer("processing message is too long", false)
var ErrExternalServiceUnavailable = consumer.NewErrConsumer("external service unavailable", true)

func HelloHandler(ctx context.Context, msg amqp.Delivery) *consumer.ErrConsumer {
	var bodyMessage MessageBody
	err := json.Unmarshal(msg.Body, &bodyMessage)
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
	log.Printf("Finish processing: %s; key: %s", msg.Body, msg.RoutingKey)

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

	helloWorldConsumer, err := consumer.NewConsumer(
		consumer.WithConnection(conn),
		consumer.WithQueue("hello.world"),
		consumer.WithQtyRoutines(5),
		consumer.WithHandler(HelloHandler),
	)
	if err != nil {
		log.Fatal(err)
	}

	helloZorldConsumer, err := consumer.NewConsumer(
		consumer.WithConnection(conn),
		consumer.WithQueue("hello.zorld"),
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

	err = helloZorldConsumer.Consume(ctx)
	if err != nil {
		log.Fatal(err)
	}

	forever := make(chan bool)
	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
