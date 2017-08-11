package main

import "fmt"

import "github.com/streadway/amqp"

import "github.com/furvr/go-mcg"

// ---

func main() {
	var err error
	var broker *mcg.Broker

	var url = "amqp://ugw_rw:2jM7ddR5KgsSsWXFyv2ecHUS@127.0.0.1:5672"

	if broker, err = mcg.NewAMQPBroker(url); err != nil {
		fmt.Errorf("Couldn't connect to AMQP: %v", err)
	}

	defer broker.Close()

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------

	conn, err := amqp.Dial("amqp://ugw_rw:2jM7ddR5KgsSsWXFyv2ecHUS@127.0.0.1:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"mail",  // name
		"topic", // type
		true,    // durable
		true,    // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		"test", // routing key
		"mail", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever = make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf("* Waiting for messages. To exit press CTRL+C")

	<-forever
}
