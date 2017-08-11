package mcg

import "github.com/streadway/amqp"

// -----------------------------------------------------------------------------
// -- AMQPBroker
// -----------------------------------------------------------------------------

// AMQPBroker DOC: ..
type AMQPBroker struct {
	conn *amqp.Connection
}

// NewAMQPBroker DOC: ..
func NewAMQPBroker(url string) (*Broker, error) {
	var err error
	var agent *AMQPBroker

	if agent, err = NewAMQPBroker(url); err != nil {
		return nil, err
	}

	return NewBroker(agent).Connect(), nil
}

// ---

// Connect DOC: ..
func (b *Broker) Connect() error {
	return b.agent.Connect()
}

// Close DOC: ..
func (b *Broker) Close() {
	b.agent.Close()
}

// ---

// Send DOC: ..
func (a *AMQPBroker) Send(path string, message []byte) error {
	var err error
	var exchange string
	var route string

	if exchange, route = ParseAMQPRoute(path); exchange == "" || route == "" {
		fmt.Errorf("please gimme the thing")
	}

	if err = ch.ExchangeDeclare("mail", "topic", true, true, false, false, nil); err != nil {

	}
}

// ---

func ParseAMQPRoute(path) (string, string) {

}
