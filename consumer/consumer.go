package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

var (
	DefaultQtyRoutines = 1
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
	qtyRoutines  int
	autoAck      bool
	exclusive    bool
	noLocal      bool
	noWait       bool
	args         amqp.Table
	ctrlRoutines chan bool
	ctrlShutdown sync.WaitGroup
}

func New(options ...Option) (*DefaultConsumer, error) {
	consumer := &DefaultConsumer{
		qtyRoutines: DefaultQtyRoutines,
	}

	for _, opt := range options {
		consumer = opt(consumer)
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
	consumer.ctrlRoutines = make(chan bool, consumer.qtyRoutines)

	var err error
	consumer.channel, err = consumer.conn.Channel(consumer.qtyRoutines)
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

	c.ctrlShutdown.Add(1)
	go func() {
		for msg := range msgs {
			c.dispatcher(ctx, msg, c.handler)
		}

		c.ctrlShutdown.Done()
	}()

	return nil
}

func (c *DefaultConsumer) dispatcher(ctx context.Context, msg amqp.Delivery, handler MessageHandler) {
	c.ctrlRoutines <- true

	go func(delivery amqp.Delivery, handler MessageHandler) {
		defer func() { <-c.ctrlRoutines }()

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
	}(msg, handler)
}

func (c *DefaultConsumer) Shutdown(ctx context.Context) error {
	err := c.channel.Cancel(c.name, false)
	if err != nil && err != amqp.ErrClosed {
		// TODO: this error must be logged
	}

	defer c.channel.Close() // TODO: this error must be logged

	c.ctrlShutdown.Wait()

	done := make(chan struct{}, 1)

	go func() {
		for {
			if len(c.ctrlRoutines) == 0 {
				done <- struct{}{}
			}
		}
	}()

	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
