// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Shutdown is an autogenerated mock type for the Shutdown type
type Shutdown struct {
	mock.Mock
}

// Execute provides a mock function with given fields: ctx
func (_m *Shutdown) Execute(ctx context.Context) {
	_m.Called(ctx)
}
