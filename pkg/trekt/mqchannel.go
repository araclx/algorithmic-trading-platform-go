// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

// MqChannel represents a message querying channel of TREKT-network.
type MqChannel interface {
	// Close closes the channel.
	Close()

	// CreateRPCClient creates a new instance of RPC-client.
	CreateRPCClient() (RPCClient, error)

	CreateHeartbeatServer() (MqHeartbeatServer, error)
	CreateHeartbeatClient() (MqHeartbeatClient, error)

	// Publish publishes message.
	Publish(key string, mandatory, immediate bool, message amqp.Publishing) error

	// Respond sends responce.
	Respond(amqp.Delivery)

	RegisterHandlers(
		request amqp.Publishing,
		handleResponse func(amqp.Delivery),
		handleError func(error))

	QueueDeclare(
		name string,
		durable bool,
		autoDelete bool,
		exclusive bool,
		noWait bool,
		args amqp.Table) (amqp.Queue, error)

	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)

	QueueBind(name string, key string, noWait bool, args amqp.Table) error

	Cancel(consumer string, noWait bool) error

	Consume(
		queue,
		consumer string,
		autoAck,
		exclusive,
		noLocal,
		noWait bool,
		args amqp.Table) (<-chan amqp.Delivery, error)
}

type mqChannel struct {
	trekt        Trekt
	name         string
	channel      *amqp.Channel
	returnChan   chan amqp.Return
	handlersChan chan struct {
		request        amqp.Publishing
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	}
	responseChan chan amqp.Delivery
}

func (channel *mqChannel) init(
	name string,
	kind string,
	trekt Trekt,
	mq *Mq,
	capacity uint16) error {

	channel.trekt = trekt
	channel.name = name

	var err error
	channel.channel, err = mq.conn.Channel()
	if err != nil {
		return err
	}
	channel.returnChan = channel.channel.NotifyReturn(
		make(chan amqp.Return, 1))

	err = channel.channel.ExchangeDeclare(
		channel.name,
		kind,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		channel.Close()
		return err
	}

	channel.handlersChan = make(chan struct {
		request        amqp.Publishing
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	}, capacity)
	channel.responseChan = make(chan amqp.Delivery)

	go channel.runReturnsReading()

	return nil
}

func (channel *mqChannel) Close() {
	if channel.responseChan != nil {
		close(channel.responseChan)
	}
	if channel.handlersChan != nil {
		close(channel.handlersChan)
	}
	closeChannel(&channel.channel)
}

func (channel *mqChannel) CreateRPCClient() (RPCClient, error) {
	return createMqRPCClient(channel, channel.trekt)
}

func (channel *mqChannel) CreateHeartbeatServer() (MqHeartbeatServer, error) {
	return CreateMqHeartbeatServer(channel, channel.trekt)
}

func (channel *mqChannel) CreateHeartbeatClient() (MqHeartbeatClient, error) {
	return CreateMqHeartbeatClient(channel, channel.trekt)
}

func (channel *mqChannel) Publish(
	key string, mandatory, immediate bool, message amqp.Publishing) error {

	return channel.channel.Publish(
		channel.name, key, mandatory, immediate, message)
}

func (channel *mqChannel) runReturnsReading() {

	handlers := make(map[string]struct {
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	})

loop:
	for {
		select {

		case notification, isOpened := <-channel.returnChan:
			if !isOpened {
				break loop
			}
			handler, hasHandler := handlers[notification.CorrelationId]
			if !hasHandler {
				if notification.Exchange == channel.name {
					channel.trekt.LogErrorf(
						`Failed to request RPC on exchange "%s": "%s" (code: %d).`,
						channel.name, notification.ReplyText, notification.ReplyCode)
				}
				break
			}
			delete(handlers, notification.CorrelationId)
			go handler.handleError(fmt.Errorf(
				`Failed to request RPC on exchange "%s": "%s" (code: %d)`,
				notification.Exchange, notification.ReplyText, notification.ReplyCode))

		case response, isOpened := <-channel.responseChan:
			if !isOpened {
				break
			}
			handler, hasHandler := handlers[response.CorrelationId]
			if !hasHandler {
				channel.trekt.LogError("RPC-client does not have required RPC-handler.")
				break
			}
			delete(handlers, response.CorrelationId)
			go handler.handleResponse(response)

		case request, isOpened := <-channel.handlersChan:
			if !isOpened {
				break
			}
			handlers[request.request.CorrelationId] = struct {
				handleResponse func(amqp.Delivery)
				handleError    func(error)
			}{
				handleResponse: request.handleResponse,
				handleError:    request.handleError,
			}
		}
	}

	for _, handler := range handlers {
		go handler.handleError(
			errors.New("RPC-client is stopped, request result is unknown"))
	}

}

func (channel *mqChannel) RegisterHandlers(
	request amqp.Publishing,
	handleResponse func(amqp.Delivery),
	handleError func(error)) {

	channel.handlersChan <- struct {
		request        amqp.Publishing
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	}{request: request, handleResponse: handleResponse, handleError: handleError}
}

func (channel *mqChannel) Respond(response amqp.Delivery) {
	response.Exchange = channel.name
	channel.responseChan <- response
}

func (channel *mqChannel) QueueDeclare(
	name string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args amqp.Table) (amqp.Queue, error) {

	return channel.channel.QueueDeclare(
		name, durable, autoDelete, exclusive, noWait, args)
}

func (channel *mqChannel) QueueDelete(
	name string, ifUnused, ifEmpty, noWait bool) (int, error) {

	return channel.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (channel *mqChannel) QueueBind(
	name, key string, noWait bool, args amqp.Table) error {

	return channel.channel.QueueBind(name, key, channel.name, noWait, args)
}

func (channel *mqChannel) Cancel(consumer string, noWait bool) error {
	return channel.channel.Cancel(consumer, noWait)
}

func (channel *mqChannel) Consume(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args amqp.Table) (<-chan amqp.Delivery, error) {

	return channel.channel.Consume(
		queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}
