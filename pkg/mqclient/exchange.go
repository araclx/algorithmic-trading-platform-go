// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

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
	reportError func(string)) error {
	exchange.name = name

	var err error
	exchange.channel, err = conn.Channel()
	if err != nil {
		return err
	}
	exchange.returnChan = exchange.channel.NotifyReturn(
		make(chan amqp.Return, 100))

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
	})
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
						fmt.Sprintf(`Failed to request RPC: "%s" (code: %d)`,
							notification.ReplyText, notification.ReplyCode))
				}
				break
			}
			delete(handlers, notification.CorrelationId)
			err := fmt.Errorf(`Failed to request RPC: "%s" (code: %d)`,
				notification.ReplyText, notification.ReplyCode)
			exchange.reportError(err.Error())
			go handler.handleError(err)

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
