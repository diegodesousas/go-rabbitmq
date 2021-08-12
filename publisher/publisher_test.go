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
		channel.On("Close").Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(false)
		conn.On("Close").Return(nil)

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

		err = publisher.Shutdown()
		assertions.Nil(err)

		conn.AssertNumberOfCalls(t, "Channel", 1)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		channel.AssertNumberOfCalls(t, "Confirm", 1)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 1)
		channel.AssertNumberOfCalls(t, "Publish", 1)
		channel.AssertNumberOfCalls(t, "Close", 1)
	})

	t.Run("should return error when message not confirmed", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		channel.On("Close").Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(false)

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
		channel.On("Close").Return(nil)

		channelErr := errors.New("amqp: error channel")
		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(nil, channelErr)
		conn.On("IsClosed").Return(false)

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
		channel.On("Close").Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(false)

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
		channel.On("Close").Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(false)

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
		channel.On("Close").Return(nil)

		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(false)

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

	t.Run("should try reconnect and publish confirmed message successfully", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		channel.On("Close").Return(nil)

		reconn := new(mocks.Connection)
		reconn.On("Channel", mock.Anything).Return(channel, nil)
		reconn.On("IsClosed").Return(false)

		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(true)
		conn.On("Reconnect").Return(reconn, nil)

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

		conn.AssertNumberOfCalls(t, "Channel", 0)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Reconnect", 1)

		reconn.AssertNumberOfCalls(t, "Channel", 1)
		reconn.AssertNumberOfCalls(t, "IsClosed", 0)
		reconn.AssertNumberOfCalls(t, "Reconnect", 0)

		channel.AssertNumberOfCalls(t, "Confirm", 1)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 1)
		channel.AssertNumberOfCalls(t, "Publish", 1)
		channel.AssertNumberOfCalls(t, "Close", 1)
	})

	t.Run("should try reconnect and get an error", func(t *testing.T) {
		assertions := assert.New(t)

		confirmations := make(chan amqp.Confirmation, 1)

		channel := new(mocks.Channel)
		channel.On("Confirm", mock.Anything).Return(nil)
		channel.On("NotifyPublish", mock.Anything).Return(confirmations)
		channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		channel.On("Close").Return(nil)

		expectedErr := errors.New("reconnection error")
		conn := new(mocks.Connection)
		conn.On("Channel", mock.Anything).Return(channel, nil)
		conn.On("IsClosed").Return(true)
		conn.On("Reconnect").Return(nil, expectedErr)

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

		assertions.NotNil(err)
		assertions.Equal(expectedErr, err)

		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Reconnect", 1)
		conn.AssertNumberOfCalls(t, "Channel", 0)

		channel.AssertNumberOfCalls(t, "Confirm", 0)
		channel.AssertNumberOfCalls(t, "NotifyPublish", 0)
		channel.AssertNumberOfCalls(t, "Publish", 0)
		channel.AssertNumberOfCalls(t, "Close", 0)
	})
}
