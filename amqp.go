package mcg

// import "github.com/streadway/amqp

// -----------------------------------------------------------------------------
// -- AMQPAgent
// -----------------------------------------------------------------------------

// AMQPAgent DOC: ..
type AMQPAgent struct {
	connection *ampq.Connection
}

// NewAMQPAgent DOC: ..
func NewAMQPAgent(url string) (*AMQPAgent, error) {
	var err error
	var connection *ampq.Connection

	if connection, err = amqp.Dial(url); err != nil {
		return err
	}

	return &AMQPAgent{
		connection: connection,
	}
}

// ---

// NewAMQPBroker DOC: ..
func NewAMQPBroker(conn string) error {
	var agent = NewAMQPAgent(conn)
}
