package consumer

import "errors"

var (
	ErrNilConnection      = errors.New("rabbit consumer: connection is nil")
	ErrConnectionIsClosed = errors.New("rabbit consumer: connection is closed")
	ErrEmptyQueue         = errors.New("rabbit consumer: queue is empty")
	ErrNilHandler         = errors.New("rabbit consumer: handler is nil")
)

type ErrConsumer struct {
	error
	requeue bool
}

func NewErrConsumer(msg string, requeue bool) *ErrConsumer {
	return &ErrConsumer{error: errors.New(msg), requeue: requeue}
}

func WrapErrConsumer(err error) *ErrConsumer {
	return &ErrConsumer{error: err, requeue: true}
}
