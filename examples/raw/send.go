package main

import "os"
import "log"
import "strings"

import "github.com/streadway/amqp"

func main() {
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
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)

	failOnError(err, "Failed to declare a queue")

	body := getUserInput(os.Args)
	err = ch.Publish(
		"mail", // exchange
		"test", // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	log.Printf("> Sent %s", body)
	failOnError(err, "Failed to publish a message")
}

// ---

func getUserInput(args []string) string {
	var s string

	if (len(args) < 3) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}

	return s
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
