package publisher

import (
	"errors"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/streadway/amqp"
)

var ErrDeliveryConfirmation = errors.New("amqp: delivery message not confirmed")

type Message struct {
	Exchange   string
	RoutingKey string
	amqp.Publishing
}

type Publisher struct {
	conn connection.Connection
}

func New(conn connection.Connection) Publisher {
	return Publisher{
		conn: conn,
	}
}

func (p Publisher) Publish(message Message) error {
	channel, err := p.conn.Channel()
	if err != nil {
		return err
	}

	if err := channel.Confirm(false); err != nil {
		return err
	}

	confirmation := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	err = channel.Publish(
		message.Exchange,
		message.RoutingKey,
		true,
		false,
		message.Publishing,
	)
	if err != nil {
		return err
	}

	if confirmed := <-confirmation; !confirmed.Ack {
		return ErrDeliveryConfirmation
	}

	return nil
}
