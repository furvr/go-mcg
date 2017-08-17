package mcg

import "fmt"

// -----------------------------------------------------------------------------
// -- Broker
// -----------------------------------------------------------------------------

// Broker DOC: ..
type Broker struct {
	Agent Agent
	done  chan bool
}

// NewBroker DOC: ..
func NewBroker(agent Agent) *Broker {
	return &Broker{Agent: agent}
}

// ---

// Start DOC: ..
func (b *Broker) Start() {
	b.done = make(chan bool)
	<-b.done
}

// Close DOC: ..
func (b *Broker) Close() {
	if b.Agent != nil {
		b.Agent.Close()
	}
}

// ---

// Send DOC: ..
func (b *Broker) Send(key string, content map[string]interface{}) error {
	if b.Agent != nil {
		return fmt.Errorf("agent not defined")
	}

	return b.Agent.Send(key, &Message{
		Data: content,
	})
}

// Handle DOC: ..
func (b *Broker) Handle(key string, limit int, handler HandlerFunc) error {
	var err_chan chan error

	if b.Agent != nil {
		go func() {
			if err := b.Agent.Receive(key, limit, handler); err != nil {
				err_chan <- err
			}
		}()
	}

	return <-err_chan
}
