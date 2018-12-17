// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"fmt"
	"log"
	"strings"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// LogMessage represents log-record delivered from a queue.
type LogMessage struct{ amqpMessage }

// GetRecord returns log-record content.
func (message *LogMessage) GetRecord() string {
	return string(message.message.Body)
}

// GetLevel returns log-record level.
func (message *LogMessage) GetLevel() string {
	index := strings.Index(message.message.RoutingKey, ".")
	if index <= 0 {
		return message.message.RoutingKey
	}
	return message.message.RoutingKey[:index]
}

// GetNodeID returns author node instance ID.
func (message *LogMessage) GetNodeID() string {
	index := strings.Index(message.message.RoutingKey, ".")
	if index > 0 {
		index++
		if index < len(message.message.RoutingKey) {
			return message.message.RoutingKey[index:]
		}
	}
	return message.message.RoutingKey
}

///////////////////////////////////////////////////////////////////////////////

// LogExchange represents logger exchange.
type LogExchange struct {
	exchange
}

func createLogExchange(conn *amqp.Connection) (*LogExchange, error) {
	result := &LogExchange{}
	err := result.exchange.init(
		"log", "topic", conn,
		func(message string) { fmt.Printf(`Log exchange error: "%s".`, message) })
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the logger exchange.
func (exchange *LogExchange) Close() {
	exchange.exchange.close()
}

// Subscribe creates a subscription to log-messages.
func (exchange *LogExchange) Subscribe(
	request string) (*LogSubscription, error) {
	return createLogSubscription(request, exchange)
}

func (exchange *LogExchange) publishf(
	client *Client, severity, format string, args ...interface{}) {
	exchange.publish(client, severity, fmt.Sprintf(format, args...))
}
func (exchange *LogExchange) publish(
	client *Client, severity, message string) {
	log.Print(severity + ":\t" + message)
	exchange.channel.Publish(
		exchange.name,
		severity+"."+client.id,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

///////////////////////////////////////////////////////////////////////////////

// LogSubscription represents subscription to logger data
type LogSubscription struct {
	messageSubscription
}

func createLogSubscription(
	request string, exchange *LogExchange) (*LogSubscription, error) {
	result := &LogSubscription{}
	err := result.messageSubscription.init(
		request, &exchange.exchange, true,
		func(message string) { log.Println(message + ".") })
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the subscription.
func (subscription *LogSubscription) Close() {
	subscription.messageSubscription.close()
}

// Handle receives messages and calls a handler for each.
func (subscription *LogSubscription) Handle(handler func(LogMessage)) {
	subscription.messageSubscription.handle(func(message amqp.Delivery) {
		handler(LogMessage{amqpMessage: amqpMessage{message: message}})
	})
}

///////////////////////////////////////////////////////////////////////////////
