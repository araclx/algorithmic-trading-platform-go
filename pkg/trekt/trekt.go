// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// Trekt manages the messages sending and receiving.
type Trekt interface {
	// Close closes the connection to TREKT.
	Close()

	// GetTypeName returns node type name.
	GetTypeName() string
	// GetID returns node ID.
	GetID() string

	// GetLogExchange returns log exchange.
	GetLogExchange() *LogExchange
	// LogErrorf formats and sends error message in the queue and prints to the
	// standard logger.
	LogErrorf(format string, args ...interface{})
	// LogError sends error message in the queue and prints to the standard
	// logger.
	LogError(message string)
	// LogWarnf formats and sends warning message in the queue and prints to the
	// standard logger.
	LogWarnf(format string, args ...interface{})
	// LogWarn sends warning message in the queue and prints to the standard
	// logger.
	LogWarn(message string)
	// LogInfof formats and sends information message in the queue and prints to
	// the standard logger.
	LogInfof(format string, args ...interface{})
	// LogInfo sends information message in the queue and prints to the standard
	// logger.
	LogInfo(message string)
	// LogDebugf formats and sends debug message in the queue and prints to the
	// standard logger.
	LogDebugf(format string, args ...interface{})
	// LogDebug sends debug message in the queue and prints prints to the
	// standard logger.
	LogDebug(message string)

	// CreateAuthExchange creates an exchange instance for authorization.
	CreateAuthExchange(capacity uint16) (*AuthExchange, error)
	// CreateAuthExchangeOrExit creates an exchange instance for authorization
	// or exits with error printing if creating is failed.
	CreateAuthExchangeOrExit(capacity uint16) *AuthExchange

	// CreateSecuritiesExchange creates an exchange instance for securities.
	CreateSecuritiesExchange(capacity uint16) (*SecuritiesExchange, error)

	// CreateSecuritiesExchangeOrExit creates an exchange instance for securities
	// or exits with error printing if creating is failed.
	CreateSecuritiesExchangeOrExit(capacity uint16) *SecuritiesExchange

	// CreateMarketDataExchange creates an exchange instance for market data.
	CreateMarketDataExchange(capacity uint16) (MarketDataExchange, error)
	// CreateMarketDataExchangeOrExit creates an exchange instance for market
	// data or exits with error printing if creating is failed.
	CreateMarketDataExchangeOrExit(capacity uint16) MarketDataExchange

	// CreateTicker starts new ticker.
	CreateTicker(duration time.Duration) Ticker
}

type trekt struct {
	typeName string
	name     string
	id       string

	mq     Mq
	stream Stream

	log *LogExchange
}

// DealOrExit creates a new connection to TREKT or exits with error printing if
// creating is failed.
func DealOrExit(
	nodeType string,
	nodeName string,
	messageQueueingBroker string,
	streamBrokers []string,
	capacity uint16) Trekt {

	result, err := Dial(
		nodeType, nodeName, messageQueueingBroker, streamBrokers, capacity)
	if err != nil {
		log.Fatalf(`Failed to connect to TREKT: "%s".`, err)
	}
	return result
}

// Dial creates a new a new connection to TREKT.
func Dial(
	nodeTypeName string,
	nodeName string,
	messageQueueingBroker string,
	streamBrokers []string,
	capacity uint16) (Trekt, error) {

	if nodeTypeName == "" {
		return nil, errors.New("Node type name is empty")
	}
	if nodeName == "" {
		return nil, errors.New("Node name is empty")
	}

	result := &trekt{
		typeName: nodeTypeName,
		name:     nodeName,
		id:       nodeTypeName + "." + nodeName,
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

	result.log, err = createLogExchange(result, &result.stream, capacity)
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

func (trekt *trekt) Close() {
	if trekt.log != nil {
		trekt.LogDebug("Closing connection to TREKT...")
		trekt.log.Close()
	}
	trekt.stream.close()
	trekt.mq.close()
}

func (trekt *trekt) GetTypeName() string { return trekt.typeName }
func (trekt *trekt) GetID() string       { return trekt.id }

func (trekt *trekt) GetLogExchange() *LogExchange { return trekt.log }

func (trekt *trekt) LogErrorf(format string, args ...interface{}) {
	trekt.log.publishf("error", format, args...)
}

func (trekt *trekt) LogError(message string) {
	trekt.log.publish("error", message)
}

func (trekt *trekt) LogWarnf(format string, args ...interface{}) {
	trekt.log.publishf("warn", format, args...)
}

func (trekt *trekt) LogWarn(message string) {
	trekt.log.publish("warn", message)
}

func (trekt *trekt) LogInfof(format string, args ...interface{}) {
	trekt.log.publishf("info", format, args...)
}

func (trekt *trekt) LogInfo(message string) {
	trekt.log.publish("info", message)
}

func (trekt *trekt) LogDebugf(format string, args ...interface{}) {
	trekt.log.publishf("debug", format, args...)
}

func (trekt *trekt) LogDebug(message string) {
	trekt.log.publish("debug", message)
}

func (trekt *trekt) CreateAuthExchange(
	capacity uint16) (*AuthExchange, error) {

	return createAuthExchange(trekt, &trekt.mq, capacity)
}

func (trekt *trekt) CreateAuthExchangeOrExit(
	capacity uint16) *AuthExchange {

	result, err := trekt.CreateAuthExchange(capacity)
	if err != nil {
		trekt.LogErrorf(`Failed to create auth-exchange: "%s".`, err)
		log.Fatalf(`Failed to create auth-exchange: "%s".`, err)
	}
	return result
}

func (trekt *trekt) CreateSecuritiesExchange(
	capacity uint16) (*SecuritiesExchange, error) {

	return createSecuritiesExchange(trekt, &trekt.mq, capacity)
}

func (trekt *trekt) CreateSecuritiesExchangeOrExit(
	capacity uint16) *SecuritiesExchange {

	result, err := trekt.CreateSecuritiesExchange(capacity)
	if err != nil {
		trekt.LogErrorf(`Failed to create securities exchange: "%s".`, err)
		log.Fatalf(`Failed to create securities exchange: "%s".`, err)
	}
	return result
}

func (trekt *trekt) CreateMarketDataExchange(
	capacity uint16) (MarketDataExchange, error) {

	return createMarketDataExchange(trekt, &trekt.mq, &trekt.stream, capacity)
}

func (trekt *trekt) CreateMarketDataExchangeOrExit(
	capacity uint16) MarketDataExchange {

	result, err := trekt.CreateMarketDataExchange(capacity)
	if err != nil {
		trekt.LogErrorf(`Failed to create MD-exchange: "%s".`, err)
		log.Fatalf(`Failed to create MD-exchange: "%s".`, err)
	}
	return result
}

func (trekt trekt) CreateTicker(duration time.Duration) Ticker {
	return &ticker{ticker: time.NewTicker(duration)}
}

func (trekt *trekt) getMq() *Mq         { return &trekt.mq }
func (trekt *trekt) getStream() *Stream { return &trekt.stream }
