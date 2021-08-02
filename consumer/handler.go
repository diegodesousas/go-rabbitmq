package consumer

import (
	"context"

	"github.com/streadway/amqp"
)

type MessageHandler func(ctx context.Context, delivery amqp.Delivery) *ErrConsumer
