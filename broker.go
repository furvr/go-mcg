package mcg

import "fmt"

// -----------------------------------------------------------------------------

// Broker DOC: ..
type Broker struct {
	Topic string
	agent Agent
}

func NewBroker(topic string, agent Agent) *Broker {
	return &Broker{
		Topic: topic,
		agent: agent,
	}
}

// ---

func (b *Broker) Start() error {
	if b.agent != nil {
		return b.agent.Done()
	}

	return fmt.Errorf("can't start broker; no agent found")
}

func (b *Broker) Close() error {
	if b.agent != nil {
		return b.agent.Close()
	}

	return nil
}

// ---

func (b *Broker) Send(key string, context Context, parts ...Part) error {
	if b.agent == nil {
		return fmt.Errorf("agent not defined")
	}

	return b.agent.Send(b.Topic, key, &Message{
		Context: context,
		Body:    &Body{Parts: parts},
	})
}

func (b *Broker) Handle(key string, limit int, handler HandlerFunc) chan error {
	var err_chan chan error

	if b.agent != nil {
		go func() {
			if err := b.agent.Receive(b.Topic, key, limit, handler); err != nil {
				err_chan <- err
			}
		}()
	}

	return err_chan
}
