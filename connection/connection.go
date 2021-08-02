package connection

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Connection interface {
	IsClosed() bool
	Channel() (Channel, error)
}

type Channel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
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
}

func (d DefaultConnection) Channel() (Channel, error) {
	channel, err := d.Connection.Channel()
	if err != nil {
		return nil, err
	}

	return Channel(channel), nil
}

func Dial(config Config) (Connection, error) {
	conn, err := amqp.Dial(fmt.Sprint(config))
	if err != nil {
		return nil, err
	}

	return DefaultConnection{conn}, nil
}
