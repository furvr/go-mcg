package mcg

import "github.com/streadway/amqp"

// -----------------------------------------------------------------------------
// -- AMQPBroker
// -----------------------------------------------------------------------------

// AMQPBroker DOC: ..
type AMQPBroker struct {
	Exchange AMQPExchange `json:"exchange"`
	url      string
	conn     *amqp.Connection
}

// AMQPExchange DOC: ..
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool `json:"auto_delete"`
	Internal   bool
	NoWait     bool `json:"no_wait"`
	Arguments  interface{}
}

// NewAMQPBroker DOC: ..
func NewAMQPBroker(url string) (*Broker, error) {
	var broker = &AMQPBroker{url: url}
	return broker, broker.Connect()
}

// ---

// Connect DOC: ..
func (b *Broker) Connect() error {
	var err error

	if b.conn, err = amqp.Dial(b.url); err != nil {
		return err
	}

	return nil
}

// Close DOC: ..
func (b *Broker) Close() {
	b.conn.Close()
}

func (b *Broker) ensure() error {
	var err error

	if b.conn == nil {
		if err = b.Connect(); err != nil {
			return err
		}
	}

	return ch.ExchangeDeclare(
		b.Exchange.Name,
		b.Exchange.Type,
		b.Exchange.Durable,
		b.Exchange.AutoDelete,
		b.Exchange.Internal,
		b.Exchange.NoWait,
		b.Exchange.Arguments,
	)
}

// ---

// Send DOC: ..
func (a *AMQPBroker) Send(key string, message []byte) error {
	var err error

	if err = b.ensure(); err != nil {
		return err
	}

	// ..
}
