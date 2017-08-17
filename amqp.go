package mcg

import "fmt"
import "encoding/json"

import "github.com/streadway/amqp"

// -----------------------------------------------------------------------------
// -- AMQPAgent
// -----------------------------------------------------------------------------

// AMQPAgent DOC: ..
type AMQPAgent struct {
	URL   string `json:"url"`
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

	var route = a.Topic + ":" + key

	if _, err = amqpQueueDeclare(ch, route); err != nil {
		return err
	}

	var body []byte

	if body, err = amqpEncodeMessage(message); err != nil {
		return err
	}

	var pub = amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	}

	if err = ch.Publish("", route, false, false, pub); err != nil {
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

	if err = ch.Qos(limit, 0, false); err != nil {
		return err
	}

	var route = a.Topic + ":" + key

	if _, err = amqpQueueDeclare(ch, route); err != nil {
		return err
	}

	var consumer = route
	var deliveries <-chan amqp.Delivery

	// TODO: Check that these arguments work for handling work queue messages
	// ARGS: queue, consumer, auto-ack, exclusive, no-local, no-wait, args
	if deliveries, err = ch.Consume(route, consumer, false, false, false, false, nil); err != nil {
		return err
	}

	for d := range deliveries {
		go a.procDelivery(d, handler)
	}

	return nil
}

func (a *AMQPAgent) procDelivery(delivery amqp.Delivery, handler HandlerFunc) {
	var err error
	var message *Message

	if message, err = amqpDecodeMessage(delivery.Body); err != nil {
		delivery.Nack(false, false)
		return
	}

	if err = handler(message); err != nil {
		delivery.Nack(false, true)
		return
	}

	delivery.Ack(false)
}

// ---

func amqpDecodeMessage(body []byte) (*Message, error) {
	var err error
	var message Message

	if err = json.Unmarshal(body, &message); err != nil {
		return nil, err
	}

	return &message, nil
}

func amqpEncodeMessage(message *Message) ([]byte, error) {
	var err error
	var body []byte

	if body, err = json.Marshal(message); err != nil {
		return nil, err
	}

	return body, nil
}

// ---

func amqpExchangeDeclare(ch *amqp.Channel, topic string) error {
	// TODO: Ensure this doesn't drop exchange topics at unexpected times.
	// Args: name, kind, durable, autoDelete, internal, noWait, args
	return ch.ExchangeDeclare(topic, "topic", true, false, false, false, nil)
}

func amqpQueueBind(ch *amqp.Channel, topic, key string) (*amqp.Queue, error) {
	var err error

	// NOTE: We're only doing this when we successfully instantiate connections.
	if err = amqpExchangeDeclare(ch, topic); err != nil {
		return nil, err
	}

	var queue amqp.Queue
	var name = topic + ":" + key

	// Args: name, durable, autoDelete, exclusive, noWait, args
	if queue, err = amqpQueueDeclare(ch, name); err != nil {
		return nil, err
	}

	// Args: name, key, exchange, noWait, args
	if err = ch.QueueBind(name, key, topic, false, nil); err != nil {
		return nil, err
	}

	return &queue, nil
}

// ---

func amqpQueueDeclare(ch *amqp.Channel, queue string) (amqp.Queue, error) {
	// Args: name, durable, autoDelete, exclusive, noWait, args
	return ch.QueueDeclare(queue, true, false, false, false, nil)
}
