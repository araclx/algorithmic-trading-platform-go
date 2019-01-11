// Copyright 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

type exchange struct {
	name         string
	channel      *amqp.Channel
	returnChan   chan amqp.Return
	handlersChan chan struct {
		request        amqp.Publishing
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	}
	responseChan chan amqp.Delivery
	reportError  func(string)
}

func (exchange *exchange) init(
	name, kind string,
	conn *amqp.Connection,
	capacity uint16,
	reportError func(string)) error {
	exchange.name = name

	var err error
	exchange.channel, err = conn.Channel()
	if err != nil {
		return err
	}
	exchange.returnChan = exchange.channel.NotifyReturn(
		make(chan amqp.Return, 1))

	err = exchange.channel.ExchangeDeclare(
		exchange.name,
		kind,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		exchange.close()
		return err
	}

	exchange.handlersChan = make(chan struct {
		request        amqp.Publishing
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	}, capacity)
	exchange.responseChan = make(chan amqp.Delivery)

	exchange.reportError = reportError

	go exchange.runReturnsReading()

	return nil
}

func (exchange *exchange) close() {
	if exchange.responseChan != nil {
		close(exchange.responseChan)
	}
	if exchange.handlersChan != nil {
		close(exchange.handlersChan)
	}
	closeChannel(&exchange.channel)
}

func (exchange *exchange) publish(
	key string, mandatory, immediate bool, message amqp.Publishing) error {

	return exchange.channel.Publish(
		exchange.name, key, mandatory, immediate, message)
}

func (exchange *exchange) runReturnsReading() {

	handlers := make(map[string]struct {
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	})

loop:
	for {
		select {

		case notification, isOpened := <-exchange.returnChan:
			if !isOpened {
				break loop
			}
			handler, hasHandler := handlers[notification.CorrelationId]
			if !hasHandler {
				if notification.Exchange == exchange.name {
					exchange.reportError(
						fmt.Sprintf(
							`Failed to request RPC on exchange "%s": "%s" (code: %d)`,
							exchange.name, notification.ReplyText, notification.ReplyCode))
				}
				break
			}
			delete(handlers, notification.CorrelationId)
			go handler.handleError(fmt.Errorf(
				`Failed to request RPC on exchange "%s": "%s" (code: %d)`,
				notification.Exchange, notification.ReplyText, notification.ReplyCode))

		case response, isOpened := <-exchange.responseChan:
			if !isOpened {
				break
			}
			handler, hasHandler := handlers[response.CorrelationId]
			if !hasHandler {
				exchange.reportError("RPC-client does not have required RPC-handler")
				break
			}
			delete(handlers, response.CorrelationId)
			go handler.handleResponse(response)

		case request, isOpened := <-exchange.handlersChan:
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
