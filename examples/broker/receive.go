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
}
