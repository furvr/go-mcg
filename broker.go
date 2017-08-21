package mcg

import "fmt"

// -----------------------------------------------------------------------------
// -- Broker
// -----------------------------------------------------------------------------

// Broker DOC: ..
type Broker struct {
	agent Agent
}

// NewBroker DOC: ..
func NewBroker(agent Agent) *Broker {
	return &Broker{agent: agent}
}

// ---

// Start DOC: ..
func (b *Broker) Start() error {
	return b.agent.Done()
}

// Close DOC: ..
func (b *Broker) Close() error {
	if b.agent != nil {
		return b.agent.Close()
	}

	return fmt.Errorf("no agent found; cannot close any further")
}

// ---

// Send DOC: ..
func (b *Broker) Send(key string, context Context, body Body) error {
	if b.agent == nil {
		return fmt.Errorf("agent not defined")
	}

	return b.agent.Send(key, &Message{
		Context: context,
		Body:    body,
	})
}

// Handle DOC: ..
func (b *Broker) Handle(key string, limit int, handler HandlerFunc) chan error {
	var err_chan chan error

	if b.agent != nil {
		go func() {
			if err := b.agent.Receive(key, limit, handler); err != nil {
				err_chan <- err
			}
		}()
	}

	return err_chan
}
