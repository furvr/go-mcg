package main

import "fmt"
import "time"

import "github.com/furvr/go-mcg"

// ---

// TODO: Do we need to close this explicitly (OR ELSE!)?
var agent *mcg.AMQPAgent

// ---

func init() {
	var err error

	if agent, err = mcg.NewAMQPAgent("amqp://ugw_rw:2jM7ddR5KgsSsWXFyv2ecHUS@127.0.0.1:5672", "auth", "mail"); err != nil {
		fmt.Errorf("Couldn't connect to AMQP: %v", err)
	}
}

func main() {
	var broker = mcg.NewBroker(agent)

	broker.Handle("log.debug", 3000, testHandlerOne)
	broker.Handle("dothing", 1, testHandlerTwo)

	broker.Start()
}

// ---

func testHandlerOne(msg *mcg.Message) error {
	return testHandler(msg, "log.debug")
}

func testHandlerTwo(msg *mcg.Message) error {
	return testHandler(msg, "dothing")
}

func testHandler(msg *mcg.Message, iter string) error {
	fmt.Printf("Starting `%v`: %v\n", iter, string(msg.Body))
	time.Sleep(time.Duration(3) * time.Second)
	fmt.Printf("Finished `%v`: %v\n", iter, string(msg.Body))

	return nil
}
