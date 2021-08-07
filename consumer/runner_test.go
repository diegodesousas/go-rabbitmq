package consumer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/diegodesousas/go-rabbitmq/consumer"
	mocks "github.com/diegodesousas/go-rabbitmq/mocks/consumer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRun(t *testing.T) {
	t.Run("should run consumers successfully", func(t *testing.T) {
		assertions := assert.New(t)

		consumer1 := new(mocks.Consumer)
		consumer1.On("Consume", mock.Anything).Return(nil)
		consumer1.On("Shutdown", mock.Anything).Return(nil)

		consumer2 := new(mocks.Consumer)
		consumer2.On("Consume", mock.Anything).Return(nil)
		consumer2.On("Shutdown", mock.Anything).Return(nil)

		ctx := context.Background()

		shutdown, err := consumer.Run(
			ctx,
			consumer1,
			consumer2,
		)

		assertions.Nil(err)

		shutdown(ctx)

		consumer1.AssertNumberOfCalls(t, "Consume", 1)
		consumer1.AssertNumberOfCalls(t, "Shutdown", 1)

		consumer2.AssertNumberOfCalls(t, "Consume", 1)
		consumer2.AssertNumberOfCalls(t, "Shutdown", 1)
	})

	t.Run("should run consumers successfully with error on consume", func(t *testing.T) {
		assertions := assert.New(t)

		expectedErr := errors.New("any error")

		consumer1 := new(mocks.Consumer)
		consumer1.On("Consume", mock.Anything).Return(expectedErr)
		consumer1.On("Shutdown", mock.Anything).Return(nil)

		consumer2 := new(mocks.Consumer)
		consumer2.On("Consume", mock.Anything).Return(nil)
		consumer2.On("Shutdown", mock.Anything).Return(nil)

		ctx := context.Background()

		shutdown, err := consumer.Run(
			ctx,
			consumer1,
			consumer2,
		)

		assertions.NotNil(err)
		assertions.Equal(expectedErr, err)

		shutdown(ctx)

		consumer1.AssertNumberOfCalls(t, "Consume", 1)
		consumer1.AssertNumberOfCalls(t, "Shutdown", 1)

		consumer2.AssertNumberOfCalls(t, "Consume", 0)
		consumer2.AssertNumberOfCalls(t, "Shutdown", 1)
	})

	t.Run("should run consumers successfully with error on consume", func(t *testing.T) {
		assertions := assert.New(t)

		expectedErr := errors.New("any error")

		consumer1 := new(mocks.Consumer)
		consumer1.On("Consume", mock.Anything).Return(nil)
		consumer1.On("Shutdown", mock.Anything).Return(expectedErr)

		consumer2 := new(mocks.Consumer)
		consumer2.On("Consume", mock.Anything).Return(nil)
		consumer2.On("Shutdown", mock.Anything).Return(nil)

		ctx := context.Background()

		shutdown, err := consumer.Run(
			ctx,
			consumer1,
			consumer2,
		)

		assertions.Nil(err)

		shutdown(ctx)

		consumer1.AssertNumberOfCalls(t, "Consume", 1)
		consumer1.AssertNumberOfCalls(t, "Shutdown", 1)

		consumer2.AssertNumberOfCalls(t, "Consume", 1)
		consumer2.AssertNumberOfCalls(t, "Shutdown", 1)
	})
}
