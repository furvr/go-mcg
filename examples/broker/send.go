package main

import "os"
import "fmt"
import "strings"

import "github.com/furvr/go-mcg"

// ---

var topic string
var key string

// TODO: Do we need to close this explicitly (OR ELSE!)?
var agent *mcg.AMQPAgent

// ---

func init() {
	var err error
	var url = os.Getenv("AMQP_TEST_URL")

	if topic, err = getTopic(); err != nil {
		fmt.Printf("Error: Can't get topic: %v\n", err)
		os.Exit(0)
	}

	if key, err = getKey(); err != nil {
		fmt.Printf("Error: Can't send message: %v\n", err)
		os.Exit(0)
	}

	if agent, err = mcg.NewAMQPAgent(url, topic); err != nil {
		fmt.Errorf("Couldn't connect to AMQP: %v", err)
	}
}

func main() {
	var err error
	var body string

	if body, err = getMessageBody(); err != nil {
		fmt.Printf("Error: Can't send message: %v\n", err)
		os.Exit(0)
	}

	var message = &mcg.Message{
		Data: map[string]interface{}{
			"some_key":    body,
			"and_another": true,
			"one_more":    40,
		},
	}

	agent.Send(key, message)
	fmt.Printf("Sent message with key `%v`: %v\n", key, message.Data)
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

func getMessageBody() (string, error) {
	if len(os.Args) < 4 || os.Args[3] == "" {
		return "", fmt.Errorf("no message body provided")
	}

	return strings.Join(os.Args[3:], " "), nil
}
