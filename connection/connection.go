package connection

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Connection interface {
	IsClosed() bool
	Channel() (Channel, error)
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
	Qos(prefetch int, prefetchSize int, global bool) error
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
	*amqp.Connection
	config Config
}

func (d DefaultConnection) Reconnect() (Connection, error) {
	return Dial(d.config)
}

func (d DefaultConnection) Channel() (Channel, error) {
	return d.Connection.Channel()
}

func Dial(config Config) (Connection, error) {
	conn, err := amqp.Dial(fmt.Sprint(config))
	if err != nil {
		return nil, err
	}

	return DefaultConnection{conn, config}, nil
}
