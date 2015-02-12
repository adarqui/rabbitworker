package rabbitworker

import (
	"github.com/streadway/amqp"
	//	"log"
)

type Enqueuer interface {
	EnqueueS(string, string) error
	EnqueueB(string, []byte) error
	Shutdown()
}

type SimpleEnqueuer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	//	ack  chan uint64
	//	nack chan uint64
}

func NewSimpleEnqueuer(conn *amqp.Connection) (Enqueuer, error) {
	var err error
	se := new(SimpleEnqueuer)
	se.conn = conn
	se.ch, err = conn.Channel()
	if err != nil {
		return nil, err
	}

	//se.ch.Confirm(false)

	//se.ack, se.nack = se.ch.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

	return se, err
}

func (this *SimpleEnqueuer) Shutdown() {
	this.ch.Close()
}

func (this *SimpleEnqueuer) EnqueueS(queue string, body string) error {
	return this.EnqueueB(queue, []byte(body))
}

func (this *SimpleEnqueuer) EnqueueB(queue string, body []byte) error {
	err := this.ch.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
		})
	if err != nil {
		return err
	}
	/*
		select {
			case <-this.ack:
					  //log.Println("Acked ", tag)
			case <-this.nack:
					  //log.Println("Nack alert! ", tag)
		}
	*/
	return nil
}
