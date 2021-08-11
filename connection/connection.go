package connection

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Connection interface {
	IsClosed() bool
	Channel(prefetch int) (Channel, error)
	Close() error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Reconnect() (Connection, error)
}

type Channel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Cancel(consumer string, noWait bool) error
	Close() error
}

type Config struct {
	Username string
	Password string
	Host     string
	Port     string
	Path     string
}

func (c Config) String() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", c.Username, c.Password, c.Host, c.Port, c.Path)
}

type DefaultConnection struct {
	config Config
	*amqp.Connection
}

func (d DefaultConnection) Reconnect() (Connection, error) {
	return Dial(d.config)
}

func (d DefaultConnection) Channel(prefetch int) (Channel, error) {
	channel, err := d.Connection.Channel()
	if err != nil {
		return nil, err
	}

	if err := channel.Qos(prefetch, 0, false); err != nil {
		return nil, err
	}

	return Channel(channel), nil
}

func Dial(config Config) (Connection, error) {
	conn, err := amqp.Dial(fmt.Sprint(config))
	if err != nil {
		return nil, err
	}

	return DefaultConnection{config, conn}, nil
}
