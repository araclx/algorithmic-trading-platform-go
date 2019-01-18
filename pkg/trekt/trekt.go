// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// Trekt manages the messages sending and receiving.
type Trekt struct {
	Type string
	name string
	id   string

	mq     mq
	stream stream

	Log *LogExchange
}

// DealOrExit creates a new connection to TREKT or exits with error printing if
// creating is failed.
func DealOrExit(
	nodeType string,
	nodeName string,
	messageQueueingBroker string,
	streamBrokers []string,
	capacity uint16) *Trekt {

	result, err := Dial(
		nodeType, nodeName, messageQueueingBroker, streamBrokers, capacity)
	if err != nil {
		log.Fatalf(`Failed to connect to TREKT: "%s".`, err)
	}
	return result
}

// Dial creates a new a new connection to TREKT.
func Dial(
	nodeType string,
	nodeName string,
	messageQueueingBroker string,
	streamBrokers []string,
	capacity uint16) (*Trekt, error) {

	if nodeType == "" {
		return nil, errors.New("Node type is empty")
	}
	if nodeName == "" {
		return nil, errors.New("Node name is empty")
	}

	result := &Trekt{
		Type: nodeType,
		name: nodeName,
		id:   nodeType + "." + nodeName,
	}

	err := result.mq.init(result.id, messageQueueingBroker, "guest", "guest")
	if err != nil {
		return nil, err
	}

	err = result.stream.init(streamBrokers, result.id)
	if err != nil {
		result.mq.close()
		return nil, err
	}

	result.Log, err = createLogExchange(result, capacity)
	if err != nil {
		result.stream.close()
		result.mq.close()
		return nil, err
	}

	{
		capacityStatus := ""
		if capacity != 1 {
			capacityStatus = fmt.Sprintf(" Capacity: %d.", capacity)
		}
		result.LogDebugf(`Connected to TREKT.%s`, capacityStatus)
	}

	return result, nil
}

// Close closes the connection to TREKT.
func (trekt *Trekt) Close() {
	if trekt.Log != nil {
		trekt.LogDebug("Closing connection to TREKT...")
		trekt.Log.Close()
	}
	trekt.stream.close()
	trekt.mq.close()
}

// LogErrorf formats and sends error message in the queue and prints to the
// standard logger.
func (trekt *Trekt) LogErrorf(format string, args ...interface{}) {
	trekt.Log.publishf("error", format, args...)
}

// LogError sends error message in the queue and prints to the standard logger.
func (trekt *Trekt) LogError(message string) {
	trekt.Log.publish("error", message)
}

// LogWarnf formats and sends warning message in the queue and prints to the
// standard logger.
func (trekt *Trekt) LogWarnf(format string, args ...interface{}) {
	trekt.Log.publishf("warn", format, args...)
}

// LogWarn sends warning message in the queue and prints to the standard logger.
func (trekt *Trekt) LogWarn(message string) {
	trekt.Log.publish("warn", message)
}

// LogInfof formats and sends information message in the queue and prints to
// the standard logger.
func (trekt *Trekt) LogInfof(format string, args ...interface{}) {
	trekt.Log.publishf("info", format, args...)
}

// LogInfo sends information message in the queue and prints to the standard
// logger.
func (trekt *Trekt) LogInfo(message string) {
	trekt.Log.publish("info", message)
}

// LogDebugf formats and sends debug message in the queue and prints to the
// standard logger.
func (trekt *Trekt) LogDebugf(format string, args ...interface{}) {
	trekt.Log.publishf("debug", format, args...)
}

// LogDebug sends debug message in the queue and prints prints to the standard
// logger.
func (trekt *Trekt) LogDebug(message string) {
	trekt.Log.publish("debug", message)
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

type mq struct {
	conn          *amqp.Connection
	directChannel *amqp.Channel
	directQueue   amqp.Queue
}

func (mq *mq) init(id, broker, login, password string) error {

	var err error
	for {
		mq.conn, err = amqp.Dial(
			fmt.Sprintf("amqp://%s:%s@%s:5672/", login, password, broker))
		if err == nil {
			break
		}
		log.Printf(`Failed to connect to the message queuing broker "%s".`,
			fmt.Sprintf("amqp://%s@%s:5672/", login, broker))
		time.Sleep(5 * time.Second)
	}

	mq.directChannel, err = mq.conn.Channel()
	if err != nil {
		mq.conn.Close()
		return err
	}
	mq.directQueue, err = mq.directChannel.QueueDeclare(
		"direct."+id,
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		err = fmt.Errorf(`Failed to start unique node: "%s"`, err)
		closeChannel(&mq.directChannel)
		mq.conn.Close()
		return err
	}

	return nil
}

func (mq *mq) close() {
	log.Println("Closing MQ-client connection...")
	closeChannel(&mq.directChannel)
	err := mq.conn.Close()
	if err != nil {
		log.Printf(`MQ-client failed to close connection: "%s".`, err)
	}
}

////////////////////////////////////////////////////////////////////////////////

type stream struct {
	client sarama.Client
}

func (stream *stream) init(brokers []string, clientID string) error {

	config := sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Errors = true

	var err error
	stream.client, err = sarama.NewClient(brokers, config)
	if err != nil {
		return err
	}
	return nil
}

func (stream *stream) close() {
	log.Println("Closing stream client connection...")
	if err := stream.client.Close(); err != nil {
		log.Printf(`Stream client failed to close connection: "%s".`, err)
	}
}

////////////////////////////////////////////////////////////////////////////////
