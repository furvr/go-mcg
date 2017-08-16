package mcg

import "fmt"

import "github.com/streadway/amqp"

// -----------------------------------------------------------------------------
// -- AMQPAgent
// -----------------------------------------------------------------------------

// AMQPAgent DOC: ..
type AMQPAgent struct {
	URL   string
	Topic string `json:"topic"`
	Queue string `json:"queue"`
	Retry int
	conn  *amqp.Connection
}

// NewAMQPAgent DOC: ..
func NewAMQPAgent(url, topic, queue string) (*AMQPAgent, error) {
	var a = &AMQPAgent{
		URL:   url,
		Topic: topic,
		Queue: queue,
	}

	return a, a.Connect()
}

// NewAMQPBroker DOC: ..
func NewAMQPBroker(url, topic, queue string) (*Broker, error) {
	var err error
	var agent *AMQPAgent

	if agent, err = NewAMQPAgent(url, topic, queue); err != nil {
		return nil, err
	}

	return NewBroker(agent), nil
}

// ---

// TODO: Re-try connection when connection fails
func (a *AMQPAgent) Connect() error {
	var err error
	var watcher = make(chan *amqp.Error)

	go func() {
		var closed = <-watcher

		if err = a.Connect(); err != nil {
			panic(fmt.Sprintf("amqp went away: %v; and can't come back: %v\n", closed, err))
		}
	}()

	if a.conn, err = amqp.Dial(a.URL); err != nil {
		return err
	}

	a.conn.NotifyClose(watcher)

	return nil
}

// Close DOC: ..
func (a *AMQPAgent) Close() {
	a.conn.Close()
}

// ---

// Send DOC: ..
func (a *AMQPAgent) Send(key string, message *Message) error {
	if a.conn == nil {
		return fmt.Errorf("can't send message to route `%v:%v` to nil connection", a.Topic, key)
	}

	var err error
	var ch *amqp.Channel

	if ch, err = a.conn.Channel(); err != nil {
		return err
	}

	defer ch.Close()

	if _, err = amqpDeclareTopic(ch, a.Topic, a.Queue, key); err != nil {
		return err
	}

	var pub = amqp.Publishing{ContentType: "text/plain", Body: message.Body}

	if err = ch.Publish(a.Topic, key, false, false, pub); err != nil {
		return err
	}

	return nil
}

// Receive DOC: ..
// TODO: Response value should be error channel
func (a *AMQPAgent) Receive(key string, limit int, handler HandlerFunc) error {
	var err error
	var ch *amqp.Channel

	if ch, err = a.conn.Channel(); err != nil {
		return err
	}

	defer ch.Close()

	var queue *amqp.Queue

	if queue, err = amqpDeclareTopic(ch, a.Topic, a.Queue, key); err != nil {
		return err
	}

	var deliveries <-chan amqp.Delivery

	// TODO: Check that these arguments work for handling work queue messages
	// ARGS: queue, consumer, auto-ack, exclusive, no-local, no-wait, args
	if deliveries, err = ch.Consume(queue.Name, "", false, false, false, false, nil); err != nil {
		return err
	}

	// TODO: Replace this with handler error channel
	var count = 0
	var forever = make(chan bool)

	for d := range deliveries {
		if count < limit {
			go func() {
				var ack bool
				var err error

				var message = &Message{
					Body: d.Body,
				}

				if err = handler(message); err == nil {
					ack = true
				}

				d.Ack(ack)
			}()
		}
	}

	<-forever

	return nil
}

// ---

func amqpDeclareTopic(ch *amqp.Channel, topic, queue, key string) (*amqp.Queue, error) {
	var err error

	// NOTE: We're only doing this when we successfully instantiate connections.
	// TODO: Ensure this doesn't drop exchange topics at unexpected times.
	if err = ch.ExchangeDeclare(topic, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}

	var q amqp.Queue

	if q, err = ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return nil, err
	}

	if err = ch.QueueBind(q.Name, key, topic, false, nil); err != nil {
		return nil, err
	}

	return &q, nil
}
