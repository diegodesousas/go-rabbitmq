package consumer

import (
	"context"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/streadway/amqp"
)

var DefaultQtyRoutines = 1

type Consumer struct {
	conn        connection.Connection
	handler     MessageHandler
	queue       string
	name        string
	qtyRoutines int
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	args        amqp.Table
}

func NewConsumer(options ...Option) (*Consumer, error) {
	consumer := &Consumer{
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

	return consumer, nil
}

func (c Consumer) Consume(ctx context.Context) error {
	channel, err := c.conn.Channel()
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(
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

	ctrlRoutines := make(chan bool, c.qtyRoutines)

	go func() {
		for msg := range msgs {
			c.dispatcher(ctx, ctrlRoutines, msg, c.handler)
		}
	}()

	return nil
}

func (c Consumer) dispatcher(ctx context.Context, ctrlRoutines chan bool, msg amqp.Delivery, handler MessageHandler) {
	ctrlRoutines <- true

	go func(qtyRoutines chan bool, msg amqp.Delivery, handler MessageHandler) {
		defer func() { <-qtyRoutines }()
		errConsumer := handler(ctx, msg)

		if errConsumer == nil {
			err := msg.Ack(false)
			if err != nil {
				// TODO: this error must be logged
				return
			}

			return
		}

		err := msg.Nack(false, errConsumer.requeue)
		if err != nil {
			// TODO: this error must be logged
			return
		}
	}(ctrlRoutines, msg, handler)
}
