// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// DealOrExit creates a new TREKT-client connection with message broker or exits
// with error printing if creating is failed.
func DealOrExit(broker, nodeType, nodeName string, capacity uint16) *Trekt {
	result, err := Dial(broker, nodeType, nodeName, capacity)
	if err != nil {
		log.Fatalf(`Failed to connect to MQ broker "%s": "%s".`, broker, err)
	}
	return result
}

// Dial creates a new TREKT-client connection with message broker.
func Dial(
	broker, nodeType, nodeName string, capacity uint16) (*Trekt, error) {

	if nodeType == "" {
		return nil, errors.New("Node type is empty")
	}
	if nodeName == "" {
		return nil, errors.New("Node name is empty")
	}

	login := "guest"
	password := "guest"
	urlTemplate := `amqp://%s:%s@` + broker + `:5672/`
	connURL := fmt.Sprintf(urlTemplate, login, password)
	logURL := fmt.Sprintf(urlTemplate, login, "*****")

	var conn *amqp.Connection
	var err error
	for {
		conn, err = amqp.Dial(connURL)
		if err == nil {
			break
		}
		log.Printf(`Failed to connect to the message queuing broker "%s".`, logURL)
		time.Sleep(5 * time.Second)
	}

	result := &Trekt{
		conn: conn,
		Type: nodeType,
		name: nodeName,
		id:   nodeType + "." + nodeName,
	}
	checkResult := func() (*Trekt, error) {
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

	result.Log, err = createLogExchange(conn, capacity)
	if err != nil {
		return checkResult()
	}

	capacityStatus := ""
	if capacity != 1 {
		capacityStatus = fmt.Sprintf(" with capacity %d", capacity)
	}
	result.LogDebugf(`Connected to the message queuing broker "%s"%s.`,
		logURL, capacityStatus)

	return result, nil
}

////////////////////////////////////////////////////////////////////////////////

// Trekt manages the messages sending and receiving.
type Trekt struct {
	conn          *amqp.Connection
	Type          string
	name          string
	id            string
	directChannel *amqp.Channel
	directQueue   amqp.Queue
	Log           *LogExchange
}

// Close closes the connection.
func (trekt *Trekt) Close() {
	if trekt.Log != nil {
		trekt.LogDebug("Disconnecting from the message queuing broker...")
		trekt.Log.Close()
	}
	closeChannel(&trekt.directChannel)
	err := trekt.conn.Close()
	if err != nil {
		log.Printf(`MQ-client failed to close connection: "%s".`, err)
	}
}

// LogErrorf formats and sends error message in the queue and prints to the
// standard logger.
func (trekt *Trekt) LogErrorf(format string, args ...interface{}) {
	trekt.Log.publishf(trekt, "error", format, args...)
}

// LogError sends error message in the queue and prints to the standard logger.
func (trekt *Trekt) LogError(message string) {
	trekt.Log.publish(trekt, "error", message)
}

// LogWarnf formats and sends warning message in the queue and prints to the
// standard logger.
func (trekt *Trekt) LogWarnf(format string, args ...interface{}) {
	trekt.Log.publishf(trekt, "warn", format, args...)
}

// LogWarn sends warning message in the queue and prints to the standard logger.
func (trekt *Trekt) LogWarn(message string) {
	trekt.Log.publish(trekt, "warn", message)
}

// LogInfof formats and sends information message in the queue and prints to
// the standard logger.
func (trekt *Trekt) LogInfof(format string, args ...interface{}) {
	trekt.Log.publishf(trekt, "info", format, args...)
}

// LogInfo sends information message in the queue and prints to the standard
// logger.
func (trekt *Trekt) LogInfo(message string) {
	trekt.Log.publish(trekt, "info", message)
}

// LogDebugf formats and sends debug message in the queue and prints to the
// standard logger.
func (trekt *Trekt) LogDebugf(format string, args ...interface{}) {
	trekt.Log.publishf(trekt, "debug", format, args...)
}

// LogDebug sends debug message in the queue and prints prints to the standard
// logger.
func (trekt *Trekt) LogDebug(message string) {
	trekt.Log.publish(trekt, "debug", message)
}

// CreateAuthExchange creates an exchange instance for authorization.
func (trekt *Trekt) CreateAuthExchange(
	capacity uint16) (*AuthExchange, error) {

	return createAuthExchange(trekt, capacity)
}

// CreateAuthExchangeOrExit creates an exchange instance for authorization
// or exits with error printing if creating is failed.
func (trekt *Trekt) CreateAuthExchangeOrExit(
	capacity uint16) *AuthExchange {

	result, err := trekt.CreateAuthExchange(capacity)
	if err != nil {
		log.Fatalf(`Failed to create auth-exchange: "%s".`, err)
	}
	return result
}

// CreateSecuritiesExchange creates an exchange instance for securities.
func (trekt *Trekt) CreateSecuritiesExchange(
	capacity uint16) (*SecuritiesExchange, error) {

	return createSecuritiesExchange(trekt, capacity)
}

// CreateSecuritiesExchangeOrExit creates an exchange instance for securities
// or exits with error printing if creating is failed.
func (trekt *Trekt) CreateSecuritiesExchangeOrExit(
	capacity uint16) *SecuritiesExchange {

	result, err := trekt.CreateSecuritiesExchange(capacity)
	if err != nil {
		log.Fatalf(`Failed to create securities exchange: "%s".`, err)
	}
	return result
}

// CreateMarketDataExchange creates an exchange instance for market data.
func (trekt *Trekt) CreateMarketDataExchange(
	capacity uint16) (*MarketDataExchange, error) {

	return createMarketDataExchange(trekt, capacity)
}

// CreateMarketDataExchangeOrExit creates an exchange instance for market data
// or exits with error printing if creating is failed.
func (trekt *Trekt) CreateMarketDataExchangeOrExit(
	capacity uint16) *MarketDataExchange {

	result, err := trekt.CreateMarketDataExchange(capacity)
	if err != nil {
		log.Fatalf(`Failed to create MD-exchange: "%s".`, err)
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
