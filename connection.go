package rabbitworker

import (
	"github.com/streadway/amqp"
)

type Connection struct {
	amqp.Connection
}

func Dial(url string) (*Connection, error) {
	c, err := amqp.Dial(url)
	return &Connection{*c}, err
}
