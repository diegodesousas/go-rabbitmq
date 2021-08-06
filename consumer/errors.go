package consumer

import "errors"

var (
	ErrNilConnection      = errors.New("rabbit consumer: connection is nil")
	ErrConnectionIsClosed = errors.New("rabbit consumer: connection is closed")
	ErrEmptyQueue         = errors.New("rabbit consumer: queue is empty")
	ErrNilHandler         = errors.New("rabbit consumer: handler is nil")
)

type Error struct {
	error
	requeue bool
}

func NewError(msg string, requeue bool) *Error {
	return &Error{error: errors.New(msg), requeue: requeue}
}

func WrapErrConsumer(err error) *Error {
	return &Error{error: err, requeue: true}
}
