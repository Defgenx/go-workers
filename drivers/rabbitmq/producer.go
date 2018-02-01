package rabbitmq

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
)

// Helper => only for testing purpose | must be deleted
func Produce(body string) (*Producer, error) {
	var (
		uri          = "amqp://guest:guest@localhost:5672/"
		exchange     = "default-exchange"
		exchangeType = "direct"
		routingKey   = "default-key"
		producerKey  = "simple-producer"
		reliable     = true
	)
	producer, err := NewProducer(uri, exchange, exchangeType, routingKey, reliable, producerKey)
	if err != nil {
		fmt.Errorf("Error: %s", err)
	}

	if err := producer.Publish(body); err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("published %dB OK", len(body))

	return producer, nil
}

type Producer struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	exchange    string
	routingKey  string
	reliable    bool
	producerTag string
}

func NewProducer(amqpURI, exchange, exchangeType, routingKey string, reliable bool, producerTag string) (*Producer, error) {
	p := &Producer{
		conn:        nil,
		channel:     nil,
		exchange:    exchange,
		routingKey:  routingKey,
		reliable:    reliable,
		producerTag: producerTag}
	var err error

	log.Printf("dialing %q", amqpURI)
	p.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	p.channel, err = p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, exchange)
	if err := p.channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("exchange Declare: %s", err)
	}

	return p, nil
}

func (p *Producer) Publish(body string) error {
	// Reliable publisher confirms require confirm.select support from the connection.
	if p.reliable {
		log.Printf("enabling publishing confirms.")
		if err := p.channel.Confirm(false); err != nil {
			return fmt.Errorf("channel could not be put into confirm mode: %s", err)
		}

		confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer p.confirmOne(confirms)
	}

	log.Printf("declared Exchange, publishing %dB body (%q)", len(body), body)
	if err := p.channel.Publish(
		p.exchange,   // publish to an exchange
		p.routingKey, // routing to 0 or more queues
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}

// ACK message on success
func (p *Producer) confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

func (p *Producer) Shutdown() error {
	// will close() the deliveries channel
	if err := p.channel.Cancel(p.producerTag, true); err != nil {
		return fmt.Errorf("producer cancel failed: %s", err)
	}

	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	return nil
}
