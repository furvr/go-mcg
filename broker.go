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
func (b *Broker) Handle(key string, limit int, handler HandlerFunc) {
	go func() {
		if err := b.agent.Receive(key, limit, handler); err != nil {
			panic(fmt.Sprintf("can't receive messages: %v\n", err))
		}
	}()
}
