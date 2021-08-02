package main

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
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

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatal(err)
		return
	}

	channel, err := conn.Channel()
	if err != nil {
		return
	}

	bodyMessage, err := json.Marshal(MessageBody{
		ProcessingTime: Duration{
			1 * time.Second,
		},
		ExternalService: false,
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	err = channel.Publish(
		"hello",
		"hello.world",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Expiration:  "10000",
			Body:        bodyMessage,
		},
	)
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Println("message send")
}
