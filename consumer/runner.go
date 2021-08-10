package consumer

import (
	"context"
	"log"
	"sync"
)

type Shutdown func(ctx context.Context)

var shutdown = func(consumers []Consumer) Shutdown {
	return func(ctx context.Context) {
		waitGroup := sync.WaitGroup{}
		waitGroup.Add(len(consumers))

		for _, c := range consumers {
			go func(c Consumer) {
				defer waitGroup.Done()

				err := c.Shutdown(ctx)
				if err != nil {
					log.Print(err) // TODO: this error must logged
				}
			}(c)
		}

		waitGroup.Wait()
	}
}

func Run(ctx context.Context, consumers ...Consumer) (Shutdown, error) {
	for _, c := range consumers {
		err := c.Consume(ctx)
		if err != nil {
			return shutdown(consumers), err
		}
	}

	return shutdown(consumers), nil
}
