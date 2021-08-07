package main

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/diegodesousas/go-rabbitmq/connection"
	"github.com/diegodesousas/go-rabbitmq/publisher"
)

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

type MessageBody struct {
	ProcessingTime  Duration `json:"processing_time"`
	ExternalService bool     `json:"external_service"`
}

func main() {
	log.Println("starting publisher")

	conn, err := connection.Dial(connection.Config{
		Username: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     "5672",
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	pub := publisher.New(conn)

	content := MessageBody{
		ProcessingTime: Duration{
			2 * time.Second,
		},
		ExternalService: false,
	}

	for i := 0; i < 500; i++ {
		err = pub.Publish(publisher.Message{
			Exchange:   "hello",
			RoutingKey: "hello.world",
			Content:    content,
		})
		if err != nil {
			log.Fatal(err)
		}

		log.Println("message send")

		err = pub.Publish(publisher.Message{
			Exchange:   "hello",
			RoutingKey: "hello.zorld",
			Content:    content,
		})
		if err != nil {
			log.Fatal(err)
		}

		log.Println("message send")
	}
}
