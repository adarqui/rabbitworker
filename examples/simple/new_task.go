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
	conn, err := rabbitworker.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	e, err := rabbitworker.NewSimpleEnqueuer(conn)

	e.EnqueueS("queue1", "This is a text message1")

	e.Shutdown()
}
