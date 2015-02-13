package main

import (
	"fmt"
	"log"

	"github.com/wricardo/rabbitworker"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	e, err := rabbitworker.NewSimpleEnqueuerDial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	e.EnqueueS("queue1", "This is a text message1")

	e.Shutdown()
}
