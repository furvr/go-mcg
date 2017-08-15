package main

import "fmt"

import "github.com/furvr/go-mcg"

// ---

// TODO: Do we need to close this explicitly (OR ELSE!)?
var agent *mcg.AMQPAgent

// ---

func init() {
	var err error

	if agent, err = mcg.NewAMQPAgent("amqp://ugw_rw:2jM7ddR5KgsSsWXFyv2ecHUS@127.0.0.1:5672", "sandbox"); err != nil {
		fmt.Errorf("Couldn't connect to AMQP: %v", err)
	}
}

func main() {
	var broker = mcg.NewBroker(agent)

	broker.Handle("test", testHandler)

	broker.Start()
}

// ---

func testHandler(msg *mcg.Message) error {
	fmt.Printf("Message: %v\n", string(msg.Body))
	return nil
}
