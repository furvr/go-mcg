package mcg

import "fmt"

// -----------------------------------------------------------------------------
// -- Broker
// -----------------------------------------------------------------------------

// Broker DOC: ..
type Broker struct {
	Routes []*Route
	agent  Agent
	done   chan bool
}

// NewBroker DOC: ..
func NewBroker(agent Agent) *Broker {
	return &Broker{agent: agent}
}

// Start DOC: ..
func (b *Broker) Start() {
	b.done = make(chan bool)
	<-b.done
}

// Close DOC: ..
func (b *Broker) Close() {
	if b.agent != nil {
		b.agent.Close()
	}
}

// Handle DOC: ..
func (b *Broker) Handle(key string, handlers ...HandlerFunc) {
	var err error
	var messages chan *Message

	if messages, err = b.agent.Receive(key); err != nil {
		panic(fmt.Sprintf("can't receive messages: %v\n", err))
	}

	go func() {
		for message := range messages {
			for _, handler := range handlers {
				if err = handler(message); err != nil {
					panic(fmt.Sprintf("can't handle message: %v\n", err))
				}
			}
		}
	}()
}
