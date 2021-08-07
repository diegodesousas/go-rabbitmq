// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	context "context"

	consumer "github.com/diegodesousas/go-rabbitmq/consumer"

	mock "github.com/stretchr/testify/mock"
)

// MessageHandler is an autogenerated mock type for the MessageHandler type
type MessageHandler struct {
	mock.Mock
}

// Execute provides a mock function with given fields: ctx, message
func (_m *MessageHandler) Execute(ctx context.Context, message consumer.Message) *consumer.Error {
	ret := _m.Called(ctx, message)

	var r0 *consumer.Error
	if rf, ok := ret.Get(0).(func(context.Context, consumer.Message) *consumer.Error); ok {
		r0 = rf(ctx, message)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*consumer.Error)
		}
	}

	return r0
}