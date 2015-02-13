package rabbitworker

import (
	"github.com/streadway/amqp"
	"sync"
)

type AckFunc func(bool) error
type HandlerFuncAck func(string, []byte, AckFunc ) error

type Queues []Queue

type Queue struct {
	Name    string
	Handler HandlerFuncAck
	c       <-chan amqp.Delivery
}

type Worker interface {
	Start() error
	Wait()
	Quit() []error
}

type SimpleWorker struct {
	Queues []Queue
	wg     *sync.WaitGroup
	conn   *Connection
	ch     *amqp.Channel
}

func NewSimpleWorker(queues []Queue, conn *Connection) (Worker, error) {
	w := new(SimpleWorker)
	w.Queues = queues
	w.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.Qos(
		3,    // prefetch count
		0,    // prefetch size
		true, // global
	)
	if err != nil {
		return nil, err
	}
	w.ch = ch
	var x sync.WaitGroup
	w.wg = &x
	return w, nil
}

func (this *SimpleWorker) declareQueues() error {
	for k, v := range this.Queues {
		q, err := this.ch.QueueDeclare(
			v.Name, // name
			true,   // durable
			false,  // delete when unused
			false,  // exclusive
			false,  // no-wait
			nil,    // arguments
		)

		if err != nil {
			return err
		}
		msgs, err := this.ch.Consume(
			q.Name, // queue
			q.Name, // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			return err
		}
		this.Queues[k].c = msgs
	}
	return nil
}

func (this *SimpleWorker) Start() error {
	err := this.declareQueues()
	if err != nil {
		return err
	}
	for _, queue := range this.Queues {
		this.wg.Add(1)
		go func(queue Queue) {
			for d := range queue.c {
				queue.Handler(queue.Name, d.Body, d.Ack)
			}
			this.wg.Done()
		}(queue)
	}
	return err
}

func (this *SimpleWorker) Wait() {
	this.wg.Wait()
}

func (this *SimpleWorker) Quit() []error {
	errors := make([]error, 0)
	for _, queue := range this.Queues {
		err := this.ch.Cancel(queue.Name, false)
		if err != nil {
			errors = append(errors, err)
		}
	}
	defer this.ch.Close()
	return errors
}
