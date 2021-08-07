package consumer

import (
	"context"
)

type Shutdown func(ctx context.Context)

var shutdown = func(consumers []*Consumer) Shutdown {
	return func(ctx context.Context) {
		for _, c := range consumers {
			err := c.Shutdown(ctx)
			if err != nil {
				// TODO: this error must logged
			}
		}
	}
}

func Run(ctx context.Context, consumers ...*Consumer) (Shutdown, error) {
	for _, c := range consumers {
		err := c.Consume(ctx)
		if err != nil {
			return shutdown(consumers), err
		}
	}

	return shutdown(consumers), nil
}
