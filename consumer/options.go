package consumer

import (
	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/streadway/amqp"
)

type Option func(consumer *DefaultConsumer)

func WithQueue(name string) Option {
	return func(consumer *DefaultConsumer) {
		consumer.queue = name
	}
}

func WithRoutines(qty int) Option {
	return func(consumer *DefaultConsumer) {
		consumer.routines = qty
	}
}

func WithPrefetch(qty int) Option {
	return func(consumer *DefaultConsumer) {
		consumer.prefetch = qty
	}
}

func WithConnection(connection connection.Connection) Option {
	return func(consumer *DefaultConsumer) {
		consumer.conn = connection
	}
}

func WithHandler(handler MessageHandler) Option {
	return func(consumer *DefaultConsumer) {
		consumer.handler = handler
	}
}

func WithAutoAck(autoAck bool) Option {
	return func(consumer *DefaultConsumer) {
		consumer.autoAck = autoAck
	}
}

func WithExclusive(exclusive bool) Option {
	return func(consumer *DefaultConsumer) {
		consumer.exclusive = exclusive
	}
}

func WithNoLocal(noLocal bool) Option {
	return func(consumer *DefaultConsumer) {
		consumer.noLocal = noLocal
	}
}

func WithNoWait(noWait bool) Option {
	return func(consumer *DefaultConsumer) {
		consumer.noWait = noWait
	}
}

func WithArgs(args amqp.Table) Option {
	return func(consumer *DefaultConsumer) {
		consumer.args = args
	}
}
