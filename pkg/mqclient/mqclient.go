// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// DealOrExit creates a new client connection with message broker, or exit
// with error printing if creating is failed.
func DealOrExit(broker, nodeType, nodeName string) *Client {
	result, err := Dial(broker, nodeType, nodeName)
	if err != nil {
		log.Fatalf(`Failed to connect to MQ broker "%s": "%s".`, broker, err)
	}
	return result
}

// Dial creates a new client connection with message broker.
func Dial(broker, nodeType, nodeName string) (*Client, error) {
	if nodeType == "" {
		return nil, errors.New("Node type is empty")
	}
	if nodeName == "" {
		return nil, errors.New("Node name is empty")
	}

	conn, err := amqp.Dial(`amqp://guest:guest@` + broker + `:5672/`)
	if err != nil {
		return nil, err
	}

	result := &Client{
		conn: conn,
		id:   nodeType + "." + nodeName,
	}
	checkResult := func() (*Client, error) {
		if err != nil {
			result.Close()
			return result, err
		}
		return result, err
	}

	result.directChannel, err = conn.Channel()
	if err != nil {
		return checkResult()
	}
	result.directQueue, err = result.directChannel.QueueDeclare(
		"direct."+result.id,
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		err = fmt.Errorf(`Failed to start unique node: "%s"`, err)
		return checkResult()
	}

	result.Log = &Exchange{name: logExchangeName}
	result.Log.channel, err = conn.Channel()
	if err != nil {
		return checkResult()
	}

	err = result.Log.channel.ExchangeDeclare(
		result.Log.name,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return checkResult()
	}

	result.LogDebug("Connected.")
	return result, nil
}

// Close closes the connection.
func (client *Client) Close() {
	if client.Log != nil {
		client.LogDebug("Disconnecting...")
		client.closeChannel(&client.Log.channel, "log")
	}
	client.closeChannel(&client.directChannel, "direct")
	err := client.conn.Close()
	if err != nil {
		log.Printf(`MQ-client failed to close connection: "%s".`, err)
	}
}

// LogErrorf formats and sends error message in the queue and prints to the standard logger.
func (client *Client) LogErrorf(format string, args ...interface{}) {
	client.publishLogRecordf("error", format, args...)
}

// LogError sends error message in the queue and prints to the standard logger.
func (client *Client) LogError(message string) {
	client.publishLogRecord("error", message)
}

// LogWarnf formats and sends warning message in the queue and prints to the standard logger.
func (client *Client) LogWarnf(format string, args ...interface{}) {
	client.publishLogRecordf("warn", format, args...)
}

// LogWarn sends warning message in the queue and prints to the standard logger.
func (client *Client) LogWarn(message string) {
	client.publishLogRecord("warn", message)
}

// LogInfof formats and sends information message in the queue and prints to the standard logger.
func (client *Client) LogInfof(format string, args ...interface{}) {
	client.publishLogRecordf("info", format, args...)
}

// LogInfo sends information message in the queue and prints to the standard logger.
func (client *Client) LogInfo(message string) {
	client.publishLogRecord("info", message)
}

// LogDebugf formats and sends debug message in the queue and prints to the standard logger.
func (client *Client) LogDebugf(format string, args ...interface{}) {
	client.publishLogRecordf("debug", format, args...)
}

// LogDebug sends debug message in the queue and prints prints to the standard logger.
func (client *Client) LogDebug(message string) {
	client.publishLogRecord("debug", message)
}

// Subscribe creates subscription.
func (client *Client) Subscribe(
	exchange *Exchange,
	request string) (*Subscription, error) {
	result := &Subscription{
		exchange:       exchange,
		stopHandleChan: make(chan struct{}),
	}
	queue, err := exchange.channel.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}
	err = result.exchange.channel.QueueBind(
		queue.Name,
		request,
		exchange.name,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	result.messageChan, err = result.exchange.channel.Consume(
		queue.Name,
		"",    // consumer
		true,  // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (client *Client) publishLogRecordf(
	severity, format string,
	args ...interface{}) {
	client.publishLogRecord(severity, fmt.Sprintf(format, args...))
}
func (client *Client) publishLogRecord(severity, message string) {
	log.Print(severity + ":\t" + message)
	client.Log.channel.Publish(
		client.Log.name,
		severity+"."+client.id,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

func (client *Client) closeChannel(channel **amqp.Channel, name string) {
	if *channel == nil {
		return
	}
	err := (*channel).Close()
	if err != nil {
		log.Printf(`MQ-client failed to close channel "%s": "%s".`, name, err)
	}
	*channel = nil
}

// Client manages the messages sending and receiving.
type Client struct {
	conn          *amqp.Connection
	id            string
	directChannel *amqp.Channel
	directQueue   amqp.Queue
	Log           *Exchange
}

///////////////////////////////////////////////////////////////////////////////

// Message describes delivered from queue message.
type Message struct{ message amqp.Delivery }

// GetBody returns message body.
func (message *Message) GetBody() []byte {
	return message.message.Body
}

// GetTopic returns message topic.
func (message *Message) GetTopic() string {
	index := strings.Index(message.message.RoutingKey, ".")
	if index <= 0 {
		return message.message.RoutingKey
	}
	return message.message.RoutingKey[:index]
}

// GetNodeID returns author node ID.
func (message *Message) GetNodeID() string {
	switch message.message.Exchange {
	case logExchangeName:
		index := strings.Index(message.message.RoutingKey, ".")
		if index > 0 {
			index++
			if index < len(message.message.RoutingKey) {
				return message.message.RoutingKey[index:]
			}
		}
	}
	return message.message.RoutingKey
}

///////////////////////////////////////////////////////////////////////////////

// Subscription manages the message queue subscription.
type Subscription struct {
	exchange       *Exchange
	stopHandleChan chan struct{}
	messageChan    <-chan amqp.Delivery
}

// Close closes the subscription.
func (subscription *Subscription) Close() {
	close(subscription.stopHandleChan)
}

// Handle starts new goroutine, receives messages in the goroutine and calls,
// handler for new messages.
func (subscription *Subscription) Handle(handler func(Message)) {
	go func() {
		for {
			select {
			case message := <-subscription.messageChan:
				handler(Message{message: message})
			case <-subscription.stopHandleChan:
				log.Panicln("XXXXXXXXXXXXXXXXXx")
				return
			}
		}
	}()
}

// Stop stops active handler.
func (subscription *Subscription) Stop() {
	subscription.stopHandleChan <- struct{}{}
}

///////////////////////////////////////////////////////////////////////////////

// Exchange describes exchange.
type Exchange struct {
	name    string
	channel *amqp.Channel
}

///////////////////////////////////////////////////////////////////////////////

const (
	logExchangeName = "log"
)

///////////////////////////////////////////////////////////////////////////////
