package publisher

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	mocks "github.com/diegodesousas/go-rabbitmq/mocks/connection"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestPublisher_Publish(t *testing.T) {
	t.Run("should publish confirmed message successfully", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel").Return(channel, nil)

		publisher := New(conn)

		message := Message{
			Exchange:   "test",
			RoutingKey: "test",
			Content:    "test",
		}

		confirmations <- amqp.Confirmation{
			DeliveryTag: 1,
			Ack:         true,
		}

		err := publisher.Publish(message)

		assertions.Nil(err)

		conn.AssertNumberOfCalls(t, "Channel", 1)
		channel.AssertNumberOfCalls(t, "Confirm", 1)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 1)
		channel.AssertNumberOfCalls(t, "Publish", 1)
	})

	t.Run("should return error when message not confirmed", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel").Return(channel, nil)

		publisher := New(conn)

		message := Message{
			Exchange:   "test",
			RoutingKey: "test",
			Content:    "test",
		}

		confirmations <- amqp.Confirmation{
			DeliveryTag: 0,
			Ack:         false,
		}

		err := publisher.Publish(message)

		assertions.NotNil(err)
		assertions.Equal(ErrDeliveryConfirmation, err)

		conn.AssertNumberOfCalls(t, "Channel", 1)
		channel.AssertNumberOfCalls(t, "Confirm", 1)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 1)
		channel.AssertNumberOfCalls(t, "Publish", 1)
	})

	t.Run("should return error if channel not available", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		channelErr := errors.New("amqp: error channel")
		conn := new(mocks.Connection)
		conn.On("Channel").Return(nil, channelErr)

		publisher := New(conn)

		message := Message{
			Exchange:   "test",
			RoutingKey: "test",
			Content:    "test",
		}

		err := publisher.Publish(message)

		assertions.NotNil(err)
		assertions.Equal(channelErr, err)

		conn.AssertNumberOfCalls(t, "Channel", 1)
		channel.AssertNumberOfCalls(t, "Confirm", 0)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 0)
		channel.AssertNumberOfCalls(t, "Publish", 0)
	})

	t.Run("should return error if channel not available in confirm mode", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channelErr := errors.New("amqp: error when put channel in confirmation mode")

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(channelErr)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel").Return(channel, nil)

		publisher := New(conn)

		message := Message{
			Exchange:   "test",
			RoutingKey: "test",
			Content:    "test",
		}

		confirmations <- amqp.Confirmation{
			DeliveryTag: 0,
			Ack:         false,
		}

		err := publisher.Publish(message)

		assertions.NotNil(err)
		assertions.Equal(channelErr, err)

		conn.AssertNumberOfCalls(t, "Channel", 1)
		channel.AssertNumberOfCalls(t, "Confirm", 1)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 0)
		channel.AssertNumberOfCalls(t, "Publish", 0)
	})

	t.Run("should return error if channel cannot publish message", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channelErr := errors.New("amqp: error when publish message")

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(channelErr)

		conn := new(mocks.Connection)
		conn.On("Channel").Return(channel, nil)

		publisher := New(conn)

		message := Message{
			Exchange:   "test",
			RoutingKey: "test",
			Content:    "test",
		}

		confirmations <- amqp.Confirmation{
			DeliveryTag: 0,
			Ack:         false,
		}

		err := publisher.Publish(message)

		assertions.NotNil(err)
		assertions.Equal(channelErr, err)

		conn.AssertNumberOfCalls(t, "Channel", 1)
		channel.AssertNumberOfCalls(t, "Confirm", 1)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 1)
		channel.AssertNumberOfCalls(t, "Publish", 1)
	})

	t.Run("should return error if content is no a valid json", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel").Return(channel, nil)

		publisher := New(conn)

		message := Message{
			Exchange:   "test",
			RoutingKey: "test",
			Content:    make(chan string),
		}

		confirmations <- amqp.Confirmation{
			DeliveryTag: 0,
			Ack:         false,
		}

		err := publisher.Publish(message)

		assertions.NotNil(err)
		assertions.IsType(&json.UnsupportedTypeError{}, err)

		conn.AssertNumberOfCalls(t, "Channel", 0)
		channel.AssertNumberOfCalls(t, "Confirm", 0)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 0)
		channel.AssertNumberOfCalls(t, "Publish", 0)
	})
}
