// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"log"

	"github.com/streadway/amqp"
)

// Client manages the messages sending and receiving.
type Client struct {
	conn          *amqp.Connection
	id            string
	directChannel *amqp.Channel
	directQueue   amqp.Queue
	Log           *LogExchange
}

// Close closes the connection.
func (client *Client) Close() {
	if client.Log != nil {
		client.LogDebug("Disconnecting from the message queuing broker...")
		client.Log.Close()
	}
	closeChannel(&client.directChannel)
	err := client.conn.Close()
	if err != nil {
		log.Printf(`MQ-client failed to close connection: "%s".`, err)
	}
}

// LogErrorf formats and sends error message in the queue and prints to the
// standard logger.
func (client *Client) LogErrorf(format string, args ...interface{}) {
	client.Log.publishf(client, "error", format, args...)
}

// LogError sends error message in the queue and prints to the standard logger.
func (client *Client) LogError(message string) {
	client.Log.publish(client, "error", message)
}

// LogWarnf formats and sends warning message in the queue and prints to the
// standard logger.
func (client *Client) LogWarnf(format string, args ...interface{}) {
	client.Log.publishf(client, "warn", format, args...)
}

// LogWarn sends warning message in the queue and prints to the standard logger.
func (client *Client) LogWarn(message string) {
	client.Log.publish(client, "warn", message)
}

// LogInfof formats and sends information message in the queue and prints to
// the standard logger.
func (client *Client) LogInfof(format string, args ...interface{}) {
	client.Log.publishf(client, "info", format, args...)
}

// LogInfo sends information message in the queue and prints to the standard
// logger.
func (client *Client) LogInfo(message string) {
	client.Log.publish(client, "info", message)
}

// LogDebugf formats and sends debug message in the queue and prints to the
// standard logger.
func (client *Client) LogDebugf(format string, args ...interface{}) {
	client.Log.publishf(client, "debug", format, args...)
}

// LogDebug sends debug message in the queue and prints prints to the standard
// logger.
func (client *Client) LogDebug(message string) {
	client.Log.publish(client, "debug", message)
}

// CreateAuthExchange creates an exchange instance for authorization.
func (client *Client) CreateAuthExchange() (*AuthExchange, error) {
	return createAuthExchange(client)
}

// CreateAuthExchangeOrExit creates an exchange instance for authorization,
// or exit with error printing if creating is failed.
func (client *Client) CreateAuthExchangeOrExit() *AuthExchange {
	result, err := client.CreateAuthExchange()
	if err != nil {
		log.Fatalf(`Failed to create auth-exchange: "%s".`, err)
	}
	return result
}
