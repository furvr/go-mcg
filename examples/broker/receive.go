package main

import "os"
import "fmt"
import "time"

import "github.com/furvr/go-mcg"

// ---

var topic string
var key string

// TODO: Do we need to close this explicitly (OR ELSE!)?
var agent *mcg.AMQPAgent

// ---

func init() {
	var err error

	if topic, err = getTopic(); err != nil {
		fmt.Printf("Error: Can't get topic: %v\n", err)
		os.Exit(0)
	}

	if key, err = getKey(); err != nil {
		fmt.Printf("Error: Can't send message: %v\n", err)
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
	broker.Handle(key, 10, testHandler(key))
	broker.Start()
}

// ---

func testHandler(iter string) mcg.HandlerFunc {
	return func(message *mcg.Message) error {
		fmt.Printf("Starting `%v`: %v\n", iter, message.Data)
		time.Sleep(time.Duration(5) * time.Second)
		fmt.Printf("Finished `%v`: %v\n", iter, message.Data)

		return nil // fmt.Errorf("omg wtf man")
	}
}

// ---

func getTopic() (string, error) {
	if len(os.Args) < 2 || os.Args[1] == "" {
		return "", fmt.Errorf("no topic provided")
	}

	return os.Args[1], nil
}

func getKey() (string, error) {
	if len(os.Args) < 3 || os.Args[2] == "" {
		return "", fmt.Errorf("no key provided")
	}

	return os.Args[2], nil
}
