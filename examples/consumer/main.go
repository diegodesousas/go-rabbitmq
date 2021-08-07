package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"

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
		consumer.WithQtyRoutines(10),
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

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	notifyErrors := conn.NotifyClose(make(chan *amqp.Error))

	go func() {
		for err := range notifyErrors {
			log.Print(err, conn.IsClosed())
			interrupt <- syscall.SIGTERM
		}
	}()

	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-interrupt
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	err = helloWorldConsumer.Shutdown(ctx)
	if err != nil {
		log.Print(err)
		return
	}
	log.Printf("consumer finished")
}
