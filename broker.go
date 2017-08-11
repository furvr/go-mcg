package mcg

// -----------------------------------------------------------------------------
// -- Broker
// -----------------------------------------------------------------------------

// Broker DOC: ..
type Broker struct {
	agent Agent `json:"-"`
}

// NewBroker DOC: ..
func NewBroker(agent Agent) *Broker {
	return &Broker{agent: agent}
}

// Connect DOC: ..
func (b *Broker) Connect() error {
	return b.agent.Connect()
}

// Close DOC: ..
func (b *Broker) Close() {
	b.agent.Close()
}
