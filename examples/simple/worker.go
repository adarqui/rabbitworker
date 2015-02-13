package main

import (
	"fmt"
	"github.com/wricardo/rabbitworker"
	"log"
)

func main() {
	queues := rabbitworker.Queues{
		rabbitworker.Queue{
			Name:    "queue1",
			Handler: HandlerQueue1,
		},
		rabbitworker.Queue{
			Name:    "queue2",
			Handler: HandlerQueue2,
		},
	}

	worker, err := rabbitworker.NewSimpleWorkerDial("amqp://guest:guest@localhost:5672/", queues)
    failOnError(err, "Failed to connect to RabbitMQ")

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	err = worker.Start()
	failOnError(err, "Failed to connect to RabbitMQ")

	worker.Wait()

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func HandlerQueue1(queue string, body []byte, ack rabbitworker.AckFunc) error {
	fmt.Println("Queue 1", string(body))
	ack(false)
	return nil
}

func HandlerQueue2(queue string, body []byte, ack rabbitworker.AckFunc) error {
	fmt.Println("Queue 2", string(body))
	ack(false)
	return nil
}

