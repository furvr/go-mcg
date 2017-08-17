package main

import "os"
import "fmt"
import "time"

import "github.com/furvr/go-mcg"

// ---

var topic string

// TODO: Do we need to close this explicitly (OR ELSE!)?
var agent *mcg.AMQPAgent

// ---

func init() {
	var err error

	if topic, err = getTopic(); err != nil {
		fmt.Printf("Error: Can't get topic: %v\n", err)
		os.Exit(0)
	}

	var url = os.Getenv("AMQP_TEST_URL")

	if agent, err = mcg.NewAMQPAgent(url, topic); err != nil {
		fmt.Printf("Error: Couldn't connect to AMQP: %v\n", err)
		os.Exit(0)
	}
}

func main() {
	var broker = mcg.NewBroker(agent)

	broker.Handle("log.debug", 10, testHandlerOne)
	broker.Handle("dothing", 10, testHandlerTwo)

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
	time.Sleep(time.Duration(5) * time.Second)
	fmt.Printf("Finished `%v`: %v\n", iter, string(msg.Body))

	return nil // fmt.Errorf("omg wtf man")
}

// ---

func getTopic() (string, error) {
	if len(os.Args) < 2 || os.Args[1] == "" {
		return "", fmt.Errorf("no topic provided")
	}

	return os.Args[1], nil
}
