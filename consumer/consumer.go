package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

var (
	DefaultRoutines = 1
	DefaultPrefetch = 10
)

type Consumer interface {
	Consume(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type DefaultConsumer struct {
	conn         connection.Connection
	channel      connection.Channel
	handler      MessageHandler
	queue        string
	name         string
	routines     int
	prefetch     int
	autoAck      bool
	exclusive    bool
	noLocal      bool
	noWait       bool
	args         amqp.Table
	routinesGate chan struct{}
}

func New(options ...Option) (*DefaultConsumer, error) {
	consumer := &DefaultConsumer{
		routines: DefaultRoutines,
		prefetch: DefaultPrefetch,
	}

	for _, opt := range options {
		opt(consumer)
	}

	if consumer.handler == nil {
		return nil, ErrNilHandler
	}

	if consumer.conn == nil {
		return nil, ErrNilConnection
	}

	if consumer.conn.IsClosed() {
		return nil, ErrConnectionIsClosed
	}

	if consumer.queue == "" {
		return nil, ErrEmptyQueue
	}

	consumer.name = fmt.Sprintf("%s:%s:%s", "go-rabbitmq", consumer.queue, uuid.New())
	consumer.routinesGate = make(chan struct{}, consumer.routines)

	var err error
	consumer.channel, err = consumer.conn.Channel(consumer.prefetch)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (c *DefaultConsumer) Consume(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		c.queue,
		c.name,
		c.autoAck,
		c.exclusive,
		c.noLocal,
		c.noWait,
		c.args,
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			c.dispatcher(ctx, msg, c.handler)
		}
	}()

	return nil
}

func (c *DefaultConsumer) dispatcher(ctx context.Context, delivery amqp.Delivery, handler MessageHandler) {
	c.routinesGate <- struct{}{}

	go func() {
		defer func() { <-c.routinesGate }()

		message := Message{
			body:       delivery.Body,
			Exchange:   delivery.Exchange,
			RoutingKey: delivery.RoutingKey,
		}

		errConsumer := handler(ctx, message)

		if errConsumer == nil {
			if err := delivery.Ack(false); err != nil {
				// TODO: this error must be logged
				return
			}

			return
		}

		if err := delivery.Reject(false); err != nil {
			// TODO: this error must be logged
			return
		}
	}()
}

func (c *DefaultConsumer) Shutdown(ctx context.Context) error {
	err := c.channel.Cancel(c.name, false)
	if err != nil {
		log.Print(err) // TODO: this error must be logged
	}

	defer c.channel.Close() // TODO: this error must be logged

	done := make(chan struct{}, 1)

	go func() {
		for {
			if len(c.routinesGate) == 0 {
				done <- struct{}{}
			}
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
