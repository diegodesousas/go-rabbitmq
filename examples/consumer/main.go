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
	time.Sleep(1 * time.Second)
	log.Printf("Finish processing: %v; Exchange: %s; RoutingKey: %s", bodyMessage, message.Exchange, message.RoutingKey)

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
	log.Println("connected to rabbitmq")

	helloWorldConsumer, err := consumer.New(
		consumer.WithConnection(conn),
		consumer.WithQueue("hello.world"),
		consumer.WithQtyRoutines(2),
		consumer.WithHandler(HelloHandler),
	)
	if err != nil {
		log.Fatal("hello.world failed bootstrap: ", err)
	}

	helloZorldConsumer, err := consumer.New(
		consumer.WithConnection(conn),
		consumer.WithQueue("hello.zorld"),
		consumer.WithQtyRoutines(4),
		consumer.WithHandler(HelloHandler),
	)
	if err != nil {
		log.Fatal("hello.zorld failed bootstrap: ", err)
	}

	ctx := context.Background()

	shutdown, err := consumer.Run(
		ctx,
		helloWorldConsumer,
		helloZorldConsumer,
	)
	if err != nil {
		shutdown(ctx)
		log.Fatal("run consumers failed: ", err)
	}

	log.Printf("waiting for messages. To exit press CTRL+C")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	closeConnListener := conn.NotifyClose(make(chan *amqp.Error))

	go func() {
		<-closeConnListener
		interrupt <- syscall.SIGTERM
	}()

	<-interrupt
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	shutdown(ctx)

	if err := conn.Close(); err != nil {
		log.Print("close connection error: ", err)
	}

	log.Printf("consumer finished")
}
