package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	mocks "github.com/diegodesousas/go-rabbitmq/mocks/connection"
	amqpmocks "github.com/diegodesousas/go-rabbitmq/mocks/github.com/streadway/amqp"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewConsumer(t *testing.T) {
	t.Run("should create consumer successfully", func(t *testing.T) {
		assertions := assert.New(t)

		channel := new(mocks.Channel)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		expectedHandler := func(ctx context.Context, message Message) *Error {
			return nil
		}

		expectedQueueName := "test"

		consumer, err := New(
			WithQueue(expectedQueueName),
			WithHandler(expectedHandler),
			WithConnection(conn),
		)

		assertions.Nil(err)
		assertions.Equal(expectedQueueName, consumer.queue)
		assertions.NotNil(consumer.handler)
		assertions.Equal(DefaultQtyRoutines, consumer.qtyRoutines)
		assertions.Equal(false, consumer.autoAck)
		assertions.Equal(false, consumer.exclusive)
		assertions.Equal(false, consumer.noWait)
		assertions.Equal(false, consumer.noLocal)
		assertions.Equal(amqp.Table(nil), consumer.args)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 1)
	})

	t.Run("should set parameters successfully", func(t *testing.T) {
		assertions := assert.New(t)

		channel := new(mocks.Channel)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		testHandler := func(ctx context.Context, message Message) *Error {
			return nil
		}

		expectedQueueName := "test"
		expectedQtyRoutines := 5
		expectedTable := amqp.Table{
			"test": 1,
		}

		consumer, err := New(
			WithQueue(expectedQueueName),
			WithHandler(testHandler),
			WithConnection(conn),
			WithQtyRoutines(expectedQtyRoutines),
			WithAutoAck(true),
			WithExclusive(true),
			WithNoWait(true),
			WithNoLocal(true),
			WithArgs(expectedTable),
		)

		assertions.Nil(err)
		assertions.Equal(expectedQueueName, consumer.queue)
		assertions.Equal(expectedQtyRoutines, consumer.qtyRoutines)
		assertions.Equal(true, consumer.autoAck)
		assertions.Equal(true, consumer.exclusive)
		assertions.Equal(true, consumer.noWait)
		assertions.Equal(true, consumer.noLocal)
		assertions.Equal(expectedTable, consumer.args)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 1)
	})

	t.Run("should return error when queue is empty", func(t *testing.T) {
		assertions := assert.New(t)

		channel := new(mocks.Channel)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel").Return(channel, nil)

		testHandler := func(ctx context.Context, message Message) *Error {
			return nil
		}

		consumer, err := New(
			WithHandler(testHandler),
			WithConnection(conn),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrEmptyQueue)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 0)
	})

	t.Run("should return error when handler is nil", func(t *testing.T) {
		assertions := assert.New(t)

		channel := new(mocks.Channel)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		consumer, err := New(
			WithQueue("test"),
			WithConnection(conn),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrNilHandler)
	})

	t.Run("should return error when connection is nil", func(t *testing.T) {
		assertions := assert.New(t)

		consumer, err := New(
			WithQueue("test"),
			WithHandler(func(ctx context.Context, message Message) *Error {
				return nil
			}),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrNilConnection)
	})

	t.Run("should return error when connection is closed", func(t *testing.T) {
		assertions := assert.New(t)

		channel := new(mocks.Channel)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(true)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		consumer, err := New(
			WithConnection(conn),
			WithQueue("test"),
			WithHandler(func(ctx context.Context, message Message) *Error {
				return nil
			}),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrConnectionIsClosed)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 0)
	})
}

func mockChanMessages(messages []amqp.Delivery) <-chan amqp.Delivery {
	// unidirectionalChan multi bidirectional chan to unidirectional chan
	bidirectionalChanMessages := make(chan amqp.Delivery, len(messages))
	unidirectionalChanMessages := make(<-chan amqp.Delivery, len(messages))

	bidirectionalChan := reflect.MakeChan(reflect.TypeOf(bidirectionalChanMessages), len(messages))

	for _, message := range messages {
		bidirectionalChan.Send(reflect.ValueOf(message))
	}

	unidirectionalChan := bidirectionalChan.Convert(reflect.TypeOf(unidirectionalChanMessages))
	return unidirectionalChan.Interface().(<-chan amqp.Delivery)
}

func TestConsumer_Consume(t *testing.T) {
	type messageContent struct {
		Value string `json:"value"`
	}

	assertions := assert.New(t)

	m1 := messageContent{
		"test_1",
	}

	m2 := messageContent{
		"test_2",
	}

	m1Bytes, err := json.Marshal(m1)
	assertions.Nil(err)

	m2Bytes, err := json.Marshal(m2)
	assertions.Nil(err)

	t.Run("should consume message successfully", func(t *testing.T) {
		assertions := assert.New(t)

		acknowledger := new(amqpmocks.Acknowledger)
		acknowledger.On("Ack", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		expectedMessages := []amqp.Delivery{
			{
				Acknowledger: acknowledger,
				Body:         m1Bytes,
			},
			{
				Body: m2Bytes,
			},
		}

		messages := mockChanMessages(expectedMessages)

		channel := new(mocks.Channel)
		channel.
			On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(messages, nil)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(expectedMessages))

		mutex := sync.Mutex{}

		var expectedHandlerCalls []messageContent
		testHandler := func(ctx context.Context, message Message) *Error {
			mutex.Lock()
			defer mutex.Unlock()

			var content messageContent
			err := message.Unmarshal(&content)
			if err != nil {
				return WrapErrConsumer(err)
			}

			expectedHandlerCalls = append(expectedHandlerCalls, content)
			waitGroup.Done()

			return nil
		}

		consumer, err := New(
			WithQueue("test"),
			WithHandler(testHandler),
			WithConnection(conn),
			WithQtyRoutines(len(expectedMessages)), // to force concurrency
		)
		assertions.Nil(err)

		err = consumer.Consume(context.Background())
		assertions.Nil(err)

		waitGroup.Wait()

		assertions.Len(expectedHandlerCalls, len(expectedMessages))
		assertions.Contains(expectedHandlerCalls, m1)
		assertions.Contains(expectedHandlerCalls, m2)

		channel.AssertNumberOfCalls(t, "Consume", 1)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 1)
		acknowledger.AssertNumberOfCalls(t, "Ack", 1)
	})

	t.Run("should consume message successfully with error on handler", func(t *testing.T) {
		assertions := assert.New(t)

		acknowledger := new(amqpmocks.Acknowledger)
		acknowledger.On("Reject", mock.Anything, mock.Anything).Return(nil)

		expectedMessages := []amqp.Delivery{
			{
				Body:         m1Bytes,
				Acknowledger: acknowledger,
			},
			{
				Body: m2Bytes,
			},
		}

		messages := mockChanMessages(expectedMessages)

		channel := new(mocks.Channel)
		channel.
			On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(messages, nil)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(expectedMessages))

		mutex := sync.Mutex{}

		var expectedHandlerCalls []messageContent
		testHandler := func(ctx context.Context, message Message) *Error {
			mutex.Lock()
			defer mutex.Unlock()

			var content messageContent
			err := message.Unmarshal(&content)
			if err != nil {
				return WrapErrConsumer(err)
			}

			expectedHandlerCalls = append(expectedHandlerCalls, content)
			waitGroup.Done()

			return NewError("err: not able to process message.", false)
		}

		consumer, err := New(
			WithQueue("test"),
			WithHandler(testHandler),
			WithConnection(conn),
			WithQtyRoutines(len(expectedMessages)), // to force concurrency
		)
		assertions.Nil(err)

		err = consumer.Consume(context.Background())
		assertions.Nil(err)

		waitGroup.Wait()

		channel.AssertNumberOfCalls(t, "Consume", 1)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 1)

		assertions.Len(expectedHandlerCalls, len(expectedMessages))
		assertions.Contains(expectedHandlerCalls, m1)
		assertions.Contains(expectedHandlerCalls, m2)
	})

	t.Run("should return error when call consume method of channel", func(t *testing.T) {
		assertions := assert.New(t)

		expectedErr := errors.New("error on call of method consume")

		channel := new(mocks.Channel)
		channel.
			On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, expectedErr)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel", mock.Anything).Return(channel, nil)

		var expectedCalls []string
		testHandler := func(ctx context.Context, message Message) *Error {
			var body string
			err := message.Unmarshal(&body)
			if err != nil {
				return WrapErrConsumer(err)
			}

			expectedCalls = append(expectedCalls, body)
			return nil
		}

		consumer, err := New(
			WithQueue("test"),
			WithHandler(testHandler),
			WithConnection(conn),
		)
		assertions.Nil(err)

		err = consumer.Consume(context.Background())
		assertions.NotNil(err)
		assertions.Equal(expectedErr, err)
		assertions.Len(expectedCalls, 0)

		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 1)
		channel.AssertNumberOfCalls(t, "Consume", 1)
	})
}
