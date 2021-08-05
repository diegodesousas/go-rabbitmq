package consumer

import (
	"context"
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

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)

		expectedHandler := func(ctx context.Context, delivery amqp.Delivery) *ErrConsumer {
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
	})

	t.Run("should set parameters successfully", func(t *testing.T) {
		assertions := assert.New(t)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)

		testHandler := func(ctx context.Context, delivery amqp.Delivery) *ErrConsumer {
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
	})

	t.Run("should return error when queue is empty", func(t *testing.T) {
		assertions := assert.New(t)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)

		testHandler := func(ctx context.Context, delivery amqp.Delivery) *ErrConsumer {
			return nil
		}

		consumer, err := New(
			WithHandler(testHandler),
			WithConnection(conn),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrEmptyQueue)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
	})

	t.Run("should return error when handler is nil", func(t *testing.T) {
		assertions := assert.New(t)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)

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
			WithHandler(func(ctx context.Context, delivery amqp.Delivery) *ErrConsumer {
				return nil
			}),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrNilConnection)
	})

	t.Run("should return error when connection is closed", func(t *testing.T) {
		assertions := assert.New(t)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(true)

		consumer, err := New(
			WithConnection(conn),
			WithQueue("test"),
			WithHandler(func(ctx context.Context, delivery amqp.Delivery) *ErrConsumer {
				return nil
			}),
		)

		assertions.Nil(consumer)
		assertions.Equal(err, ErrConnectionIsClosed)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
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
	t.Run("should consume message successfully", func(t *testing.T) {
		assertions := assert.New(t)

		acknowledger := new(amqpmocks.Acknowledger)
		acknowledger.On("Ack", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		expectedMessages := []amqp.Delivery{
			{
				Acknowledger: acknowledger,
				Body:         []byte("test_1"),
			},
			{
				Body: []byte("test_2"),
			},
		}

		messages := mockChanMessages(expectedMessages)

		channel := new(mocks.Channel)
		channel.
			On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(messages, nil)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel").Return(channel, nil)

		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(expectedMessages))

		mutex := sync.Mutex{}

		var expectedHandlerCalls []string
		testHandler := func(ctx context.Context, message amqp.Delivery) *ErrConsumer {
			mutex.Lock()
			defer mutex.Unlock()

			expectedHandlerCalls = append(expectedHandlerCalls, string(message.Body))
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
		assertions.Contains(expectedHandlerCalls, "test_1")
		assertions.Contains(expectedHandlerCalls, "test_2")

		channel.AssertNumberOfCalls(t, "Consume", 1)
		conn.AssertNumberOfCalls(t, "IsClosed", 1)
		conn.AssertNumberOfCalls(t, "Channel", 1)
		acknowledger.AssertNumberOfCalls(t, "Ack", 1)
	})

	t.Run("should consume message successfully with error on handler", func(t *testing.T) {
		assertions := assert.New(t)

		acknowledger := new(amqpmocks.Acknowledger)
		acknowledger.On("Nack", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		expectedMessages := []amqp.Delivery{
			{
				Body:         []byte("test_1"),
				Acknowledger: acknowledger,
			},
			{
				Body: []byte("test_2"),
			},
		}

		messages := mockChanMessages(expectedMessages)

		channel := new(mocks.Channel)
		channel.
			On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(messages, nil)

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel").Return(channel, nil)

		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(expectedMessages))

		mutex := sync.Mutex{}

		var expectedHandlerCalls []string
		testHandler := func(ctx context.Context, message amqp.Delivery) *ErrConsumer {
			mutex.Lock()
			defer mutex.Unlock()

			expectedHandlerCalls = append(expectedHandlerCalls, string(message.Body))
			waitGroup.Done()
			return NewErrConsumer("err: not able to process message.", false)
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
		assertions.Contains(expectedHandlerCalls, "test_1")
		assertions.Contains(expectedHandlerCalls, "test_2")
	})

	t.Run("should return error when call channel", func(t *testing.T) {
		assertions := assert.New(t)

		expectedErr := errors.New("error when try get channel")

		conn := new(mocks.Connection)
		conn.On("IsClosed").Return(false)
		conn.On("Channel").Return(nil, expectedErr)

		var expectedCalls []string
		testHandler := func(ctx context.Context, message amqp.Delivery) *ErrConsumer {
			expectedCalls = append(expectedCalls, string(message.Body))
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
		conn.On("Channel").Return(channel, nil)

		var expectedCalls []string
		testHandler := func(ctx context.Context, message amqp.Delivery) *ErrConsumer {
			expectedCalls = append(expectedCalls, string(message.Body))
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
