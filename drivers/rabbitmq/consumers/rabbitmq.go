package consumers

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

var (
	uri          = "amqp://guest:guest@localhost:5672/"
	exchange     = "default-exchange"
	exchangeType = "direct"
	queue        = "default-queue"
	bindingKey   = "default-key"
	consumerTag  = "simple-consumer"
	lifetime     = 15 * time.Second
)

func Consume(timer *time.Duration) (*RabbitMQ, <-chan string, error) {
	lifetime = *timer
	c, deliveries, err := NewRabbitMQ(uri, exchange, exchangeType, queue, bindingKey, consumerTag)

	output := c.Handle(deliveries, c.done)
	return c, output, err
}

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewRabbitMQ(amqpURI, exchange, exchangeType, queueName, key, ctag string) (*RabbitMQ, <-chan amqp.Delivery, error) {
	c := &RabbitMQ{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	log.Printf("Dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, nil, fmt.Errorf("Dial: %s", err)
	}

	log.Printf("Got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("Got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("Declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("Declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
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
		return nil, nil, fmt.Errorf("Queue Consume: %s", err)
	}

	return c, deliveries, nil
}

func (c *RabbitMQ) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func (c *RabbitMQ) Handle(deliveries <-chan amqp.Delivery, done chan error) <-chan string {
	out := make(chan string)
	go func() {
		for d := range deliveries {
			out <- fmt.Sprintf(
				"%q",
				d.Body,
			)
			d.Ack(false)
		}

		log.Println("Handle: messages channel closed")
		done <- nil
	}()

	go func() {
		if lifetime > 0 {
			log.Printf("Running for %s", lifetime)
			time.Sleep(lifetime)
			if err := c.Shutdown(); err != nil {
				log.Fatalf("Error during shutdown: %s", err)
			}
			close(out)
		}
	}()
	return out
}