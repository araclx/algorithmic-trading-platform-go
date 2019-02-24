// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

///////////////////////////////////////////////////////////////////////////////

// LogExchange represents logger exchange.
type LogExchange struct {
	streamChannel
	trekt       Trekt
	key         sarama.Encoder
	producer    sarama.AsyncProducer
	stopBarrier sync.WaitGroup
}

func createLogExchange(
	trekt Trekt, stream *Stream, capacity uint16) (*LogExchange, error) {

	result := &LogExchange{
		trekt: trekt,
		key:   sarama.StringEncoder(trekt.GetTypeName())}

	err := result.streamChannel.init(stream, result.trekt)
	if err != nil {
		return nil, err
	}

	result.producer, err = sarama.NewAsyncProducerFromClient(result.stream.client)
	if err != nil {
		result.streamChannel.Close()
		return nil, err
	}

	result.stopBarrier.Add(1)
	go func() {
		defer result.stopBarrier.Done()
		for err := range result.producer.Errors() {
			log.Printf(`Failed to publish log-record: "%s".`, err)
		}
	}()

	return result, nil
}

// Close closes the logger exchange.
func (exchange *LogExchange) Close() {
	err := exchange.producer.Close()
	if err != nil {
		log.Printf(`Failed to close log stream producer: "%s".`, err)
	}
	exchange.stopBarrier.Wait()
	exchange.streamChannel.Close()
}

// Subscribe creates a subscription to log-messages.
func (exchange *LogExchange) Subscribe() (*LogSubscription, error) {
	return createLogSubscription(exchange)
}

func (exchange *LogExchange) publishf(
	severity, format string, args ...interface{}) {

	exchange.publish(severity, fmt.Sprintf(format, args...))
}
func (exchange *LogExchange) publish(severity, record string) {
	{
		log.Print(severity + ":\t" + record)
	}
	{
		message, err := json.Marshal(logMessageData{
			NodeID: exchange.trekt.GetID(),
			Level:  severity,
			Record: record})
		if err != nil {
			log.Printf(`Failed to serialize log-record: "%s".`, err)
			return
		}
		exchange.producer.Input() <- &sarama.ProducerMessage{
			Topic: "log",
			Key:   exchange.key,
			Value: sarama.ByteEncoder(message)}
	}
}

///////////////////////////////////////////////////////////////////////////////

// LogMessage represents log-record delivered from a queue.
type LogMessage interface {
	// GetSequenceNumber returns record sequence number.
	GetSequenceNumber() int64
	// GetTime returns record time.
	GetTime() time.Time
	// GetRecord returns record content.
	GetRecord() string
	// GetLevel returns record level.
	GetLevel() string
	// GetNodeID returns author node instance ID.
	GetNodeID() string
}

type logMessageData struct{ NodeID, Level, Record string }

// LogSubscription represents subscription to logger data
type LogSubscription struct {
	streamSubscription
	consumer     sarama.ConsumerGroup
	messagesChan chan sarama.ConsumerMessage
}

func createLogSubscription(exchange *LogExchange) (*LogSubscription, error) {
	result := &LogSubscription{}
	err := result.streamSubscription.init("log", &exchange.streamChannel)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the subscription.
func (subscription *LogSubscription) Close() {
	if err := subscription.streamSubscription.Close(); err != nil {
		log.Printf(`Failed to close log stream consumer: "%s".`, err)
	}
}

// GetNextMessage returns next log-message. If there are no messages it blocks
// until a new message will arrive or until subscription will be closed.
func (subscription *LogSubscription) GetNextMessage() (
	result LogMessage, isOpened bool) {

	for {
		result := logMessage{}
		var isOpened bool
		result.message, isOpened = <-subscription.Messages()
		if !isOpened {
			return logMessage{}, false
		}
		if err := json.Unmarshal(result.message.Value, &result.data); err != nil {
			log.Printf(`Failed to parse log-record: "%s".`, err)
			continue
		}
		return result, true
	}
}

type logRecordHandler struct {
	trekt        Trekt
	messagesChan chan<- sarama.ConsumerMessage
}

func (handler *logRecordHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
func (handler *logRecordHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
func (handler *logRecordHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		handler.messagesChan <- *message
		session.MarkMessage(message, "")
	}
	return nil
}

type logMessage struct {
	message sarama.ConsumerMessage
	data    logMessageData
}

func (message logMessage) GetSequenceNumber() int64 {
	return message.message.Offset
}
func (message logMessage) GetTime() time.Time {
	return message.message.Timestamp
}
func (message logMessage) GetRecord() string {
	return message.data.Record
}
func (message logMessage) GetLevel() string {
	return message.data.Level
}
func (message logMessage) GetNodeID() string {
	return message.data.NodeID
}

///////////////////////////////////////////////////////////////////////////////
