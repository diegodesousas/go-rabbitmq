package consumer

import (
	"errors"
	"reflect"
	"testing"
)

func TestWrapErrConsumer(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want *Error
	}{
		{
			name: "wrap error must requeue message",
			args: args{
				err: errors.New("unknown error"),
			},
			want: &Error{
				error:   errors.New("unknown error"),
				requeue: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WrapErrConsumer(tt.args.err); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WrapErrConsumer() = %v, want %v", got, tt.want)
			}
		})
	}
}
