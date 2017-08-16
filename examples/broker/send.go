package main

import "fmt"

// import "time"

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
	for i, _ := range make([]int, 6) {
		stutterSend("log.debug", i)
	}

	// for i, _ := range make([]int, 6) {
	// 	stutterSend("dothing", i)
	// }
}

func stutterSend(key string, i int) {
	var message = &mcg.Message{
		Body: []byte(fmt.Sprintf("Message #%d!", i)),
	}

	agent.Send(key, message)
	fmt.Printf("Sent message with key `%v`: %v\n", key, string(message.Body))

	// time.Sleep(time.Duration(1) * time.Second)
}
