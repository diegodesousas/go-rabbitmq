package publisher

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/streadway/amqp"
)

var ErrDeliveryConfirmation = errors.New("amqp: delivery message not confirmed")

type Message struct {
	Exchange        string
	RoutingKey      string
	Content         interface{}
	Headers         amqp.Table
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
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
	if p.conn.IsClosed() {
		var err error
		p.conn, err = p.conn.Reconnect()
		if err != nil {
			return err
		}
	}

	conn := p.conn

	content, err := json.Marshal(message.Content)
	if err != nil {
		return err
	}

	channel, err := conn.Channel(0)
	if err != nil {
		return err
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return err
	}

	confirmation := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	err = channel.Publish(
		message.Exchange,
		message.RoutingKey,
		true,
		false,
		amqp.Publishing{
			Body:            content,
			Headers:         message.Headers,
			ContentType:     "application/json",
			ContentEncoding: message.ContentEncoding,
			DeliveryMode:    message.DeliveryMode,
			Priority:        message.Priority,
			CorrelationId:   message.CorrelationId,
			ReplyTo:         message.ReplyTo,
			Expiration:      message.Expiration,
			MessageId:       message.MessageId,
			Timestamp:       message.Timestamp,
			Type:            message.Type,
			UserId:          message.UserId,
			AppId:           message.AppId,
		},
	)
	if err != nil {
		return err
	}

	if confirmed := <-confirmation; !confirmed.Ack {
		return ErrDeliveryConfirmation
	}

	return nil
}
