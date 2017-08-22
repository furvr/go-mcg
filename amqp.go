package mcg

import "fmt"
import "bytes"
import "encoding/gob"
import "encoding/json"

import "github.com/streadway/amqp"

// -----------------------------------------------------------------------------
// -- AMQPAgent
// -----------------------------------------------------------------------------

// AMQPAgent DOC: ..
type AMQPAgent struct {
	URL   string
	Retry int

	conn *amqp.Connection
	done chan *amqp.Error
}

// NewAMQPAgent DOC: ..
func NewAMQPAgent(url string) (*AMQPAgent, error) {
	var a = &AMQPAgent{URL: url}
	return a, a.Connect()
}

// NewAMQPBroker DOC: ..
func NewAMQPBroker(url, topic string) (*Broker, error) {
	var err error
	var agent *AMQPAgent

	if agent, err = NewAMQPAgent(url); err != nil {
		return nil, err
	}

	return NewBroker(topic, agent), nil
}

// ---

// Connect DOC: ..
func (a *AMQPAgent) Connect() error {
	var err error

	if a.URL == "" {
		return fmt.Errorf("can't connect with empty url")
	}

	if a.conn, err = amqp.Dial(a.URL); err != nil {
		return err
	}

	a.conn.NotifyClose(a.done)

	return nil
}

// Close DOC: ..
func (a *AMQPAgent) Close() error {
	return a.conn.Close()
}

// Done DOC: ..
func (a *AMQPAgent) Done() error {
	var err = <-a.done
	return err
}

// ---

// Send DOC: ..
func (a *AMQPAgent) Send(topic, key string, message *Message) error {
	if a.conn == nil {
		return fmt.Errorf("can't send message to route `%v:%v` to nil connection", topic, key)
	}

	var err error
	var ch *amqp.Channel

	if ch, err = a.conn.Channel(); err != nil {
		return err
	}

	defer ch.Close()

	var route = topic + ":" + key

	if _, err = amqpQueueDeclare(ch, route); err != nil {
		return err
	}

	var body []byte

	if body, err = amqpEncodeJSON(message); err != nil {
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
func (a *AMQPAgent) Receive(topic, key string, limit int, handler HandlerFunc) error {
	var err error
	var ch *amqp.Channel

	if ch, err = a.conn.Channel(); err != nil {
		return err
	}

	defer ch.Close()

	if err = ch.Qos(limit, 0, false); err != nil {
		return err
	}

	var route = topic + ":" + key

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

	if message, err = amqpDecodeJSON(delivery.Body); err != nil {
		fmt.Printf("Can't decode: %v\n", err)
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

func amqpEncodeJSON(message *Message) ([]byte, error) {
	var err error
	var body []byte

	if body, err = json.Marshal(message); err != nil {
		return nil, err
	}

	return body, nil
}

func amqpDecodeJSON(body []byte) (*Message, error) {
	var err error
	var message Message

	if err = json.Unmarshal(body, &message); err != nil {
		return nil, err
	}

	return &message, nil
}

func amqpEncodeGOB(message *Message) ([]byte, error) {
	var err error
	var buf bytes.Buffer

	if err = gob.NewEncoder(&buf).Encode(message); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func amqpDecodeGOB(body []byte) (*Message, error) {
	var err error
	var message *Message
	var buf bytes.Buffer

	if _, err = buf.Write(body); err != nil {
		return nil, err
	}

	if err = gob.NewDecoder(&buf).Decode(message); err != nil {
		return nil, err
	}

	return message, nil
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
