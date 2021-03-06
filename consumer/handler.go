package consumer

import (
	"context"
	"encoding/json"
)

type Message struct {
	body       []byte
	Exchange   string
	RoutingKey string
}

func (m Message) Unmarshal(value interface{}) error {
	return json.Unmarshal(m.body, value)
}

type MessageHandler func(ctx context.Context, message Message) *Error
