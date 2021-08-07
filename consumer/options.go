package consumer

import (
	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/streadway/amqp"
)

type Option func(consumer *DefaultConsumer) *DefaultConsumer

func WithQueue(name string) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.queue = name
		return consumer
	}
}

func WithQtyRoutines(qty int) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.qtyRoutines = qty
		return consumer
	}
}

func WithConnection(connection connection.Connection) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.conn = connection
		return consumer
	}
}

func WithHandler(handler MessageHandler) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.handler = handler
		return consumer
	}
}

func WithAutoAck(autoAck bool) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.autoAck = autoAck
		return consumer
	}
}

func WithExclusive(exclusive bool) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.exclusive = exclusive
		return consumer
	}
}

func WithNoLocal(noLocal bool) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.noLocal = noLocal
		return consumer
	}
}

func WithNoWait(noWait bool) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.noWait = noWait
		return consumer
	}
}

func WithArgs(args amqp.Table) Option {
	return func(consumer *DefaultConsumer) *DefaultConsumer {
		consumer.args = args
		return consumer
	}
}
