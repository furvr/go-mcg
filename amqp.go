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
	Retry int
	conn  *amqp.Connection
}

// NewAMQPAgent DOC: ..
func NewAMQPAgent(url, topic string) (*AMQPAgent, error) {
	var a = &AMQPAgent{
		URL:   url,
		Topic: topic,
	}

	return a, a.Connect()
}

// NewAMQPBroker DOC: ..
func NewAMQPBroker(url, topic string) (*Broker, error) {
	var err error
	var agent *AMQPAgent

	if agent, err = NewAMQPAgent(url, topic); err != nil {
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

		fmt.Printf("amqp went away: %v\n", closed)

		if err = a.Connect(); err != nil {
			panic(fmt.Sprintf("and can't come back: %v\n", closed, err))
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

	if _, err = amqpDeclareExchangeQueue(ch, a.Topic, key); err != nil {
		return err
	}

	var pub = amqp.Publishing{ContentType: "text/plain", Body: message.Body}

	if err = ch.Publish(a.Topic, key, false, false, pub); err != nil {
		return err
	}

	return nil
}

// Receive DOC: ..
func (a *AMQPAgent) Receive(key string) (chan *Message, error) {
	var err error
	var ch *amqp.Channel

	if ch, err = a.conn.Channel(); err != nil {
		return nil, err
	}

	defer ch.Close()

	var queue *amqp.Queue

	if queue, err = amqpDeclareExchangeQueue(ch, a.Topic, key); err != nil {
		return nil, err
	}

	fmt.Printf("Got queue `%v` for `%v:%v`\n", queue.Name, a.Topic, key)

	var deliveries <-chan amqp.Delivery

	// TODO: Check that these arguments work for handling work queue messages
	// ARGS: queue, consumer, auto-ack, exclusive, no-local, no-wait, args
	if deliveries, err = ch.Consume(queue.Name, "", true, false, false, false, nil); err != nil {
		return nil, err
	}

	var messages = make(chan *Message)

	go func() {
		for delivery := range deliveries {
			fmt.Printf("Got delivery: %v\n", delivery)
			messages <- &Message{
				Body: delivery.Body,
			}
		}
	}()

	return messages, nil
}

// ---

func amqpDeclareExchangeQueue(ch *amqp.Channel, topic, key string) (*amqp.Queue, error) {
	var err error

	// NOTE: We're only doing this when we successfully instantiate connections.
	// TODO: Ensure this doesn't drop exchange topics at unexpected times.
	if err = ch.ExchangeDeclare(topic, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}

	var queue amqp.Queue

	if queue, err = ch.QueueDeclare("asdf", true, false, false, false, nil); err != nil {
		return nil, err
	}

	if err = ch.QueueBind(queue.Name, key, topic, false, nil); err != nil {
		return nil, err
	}

	return &queue, nil
}
