package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

var (
	lifetime = 15 * time.Second
)

// Helper => only for testing purpose | must be deleted
func Consume(timer *time.Duration) (*Consumer, <-chan string, error) {
	var (
		uri          = "amqp://guest:guest@localhost:5672/"
		exchange     = "default-exchange"
		exchangeType = "direct"
		queue        = "default-queue"
		bindingKey   = "default-key"
		consumerTag  = "simple-consumer"
	)
	c, deliveries, err := NewConsumer(uri, exchange, exchangeType, queue, bindingKey, consumerTag, timer)

	output := c.Handle(deliveries)
	return c, output, err
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
}

func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string, timer *time.Duration) (*Consumer, <-chan amqp.Delivery, error) {
	lifetime = *timer
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, nil, fmt.Errorf("dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, nil, fmt.Errorf("exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, nil, fmt.Errorf("queue Bind: %s", err)
	}

	log.Printf("queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("queue Consume: %s", err)
	}

	return c, deliveries, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	return nil
}

func (c *Consumer) Handle(deliveries <-chan amqp.Delivery) <-chan string {
	out := make(chan string)
	go func() {
		for d := range deliveries {
			out <- fmt.Sprintf(
				"%q",
				d.Body,
			)
			d.Ack(false)
		}
		log.Println("handle: messages channel closed")
	}()

	go func() {
		defer close(out)
		if lifetime > 0 {
			log.Printf("running for %s", lifetime)
			time.Sleep(lifetime)
			if err := c.Shutdown(); err != nil {
				log.Fatalf("error during shutdown: %s", err)
			}
		}
	}()
	return out
}
