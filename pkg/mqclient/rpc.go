// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"errors"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

type rpc struct {
	exchange *exchange
	queue    amqp.Queue
}

func (rpc *rpc) init(name string, exchange *exchange) error {
	rpc.exchange = exchange

	var err error
	rpc.queue, err = rpc.exchange.channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	return err
}

func (rpc *rpc) close() {}

///////////////////////////////////////////////////////////////////////////////

type rpcServer struct {
	rpc
	client       *Client
	subscription messageSubscription
}

func (server *rpcServer) init(name string, exchange *exchange, client *Client) error {
	err := server.rpc.init(name, exchange)
	if err != nil {
		return err
	}

	server.client = client

	err = server.subscription.init(
		server.queue.Name, server.exchange, true,
		func(message string) { server.client.LogError(message + ".") })
	if err != nil {
		server.rpc.close()
		return err
	}

	return nil
}

func (server *rpcServer) close() {
	server.subscription.close()
	server.rpc.close()
}

func (server *rpcServer) handle(
	handle func([]byte) (response []byte, err error)) {
	server.subscription.handle(
		func(request amqp.Delivery) {
			result, err := handle(request.Body)
			response := amqp.Publishing{CorrelationId: request.CorrelationId}
			if err != nil {
				response.ContentType = "text/plain"
				response.Body = []byte(err.Error())
			} else {
				response.ContentType = "application/json"
				response.Body = result
			}
			err = server.exchange.channel.Publish(
				server.exchange.name, // exchange
				request.ReplyTo,      // routing key
				false,                // mandatory
				false,                // immediate
				response)
			if err != nil {
				server.client.LogErrorf(
					`Failed to publish RPC-server "%s" response: "%s".`,
					server.queue.Name, err)
			}
		})
}

///////////////////////////////////////////////////////////////////////////////

type rpcService struct {
	rpc

	client *Client

	responseSubscription messageSubscription

	requestsCond *sync.Cond
	requests     map[string]amqp.Publishing
}

func (service *rpcService) init(
	name string, exchange *exchange, client *Client) error {

	err := service.rpc.init(name, exchange)
	if err != nil {
		return err
	}

	service.client = client
	service.requestsCond = sync.NewCond(&sync.Mutex{})
	service.requests = make(map[string]amqp.Publishing)

	err = service.responseSubscription.init(
		"", service.exchange, true,
		func(message string) { service.client.LogError(message + ".") })
	if err != nil {
		service.rpc.close()
		return err
	}

	go service.responseSubscription.handle(func(response amqp.Delivery) {
		service.exchange.responseChan <- response
	})

	return nil
}

func (service *rpcService) close() {
	service.responseSubscription.close()

	service.requestsCond.L.Lock()
	for _, request := range service.requests {
		service.exchange.responseChan <- amqp.Delivery{
			ContentType:   "text/plain",
			CorrelationId: request.CorrelationId,
			ReplyTo:       request.ReplyTo,
			Exchange:      service.exchange.name,
			Body:          []byte("RPC-service is stopped, request result is unknown"),
		}
	}
	for len(service.requests) > 0 {
		service.requestsCond.Wait()
	}
	service.requestsCond.L.Unlock()

	service.rpc.close()
}

func (service *rpcService) request(
	request []byte,
	handleSuccess func([]byte), handleFail func(error)) {

	message := amqp.Publishing{
		CorrelationId: "1234",
		ReplyTo:       service.responseSubscription.queue.Name,
		ContentType:   "application/json",
		Body:          request}

	service.requestsCond.L.Lock()
	service.requests[message.CorrelationId] = message
	service.requestsCond.L.Unlock()
	reportHandling := func() {
		service.requestsCond.L.Lock()
		delete(service.requests, message.CorrelationId)
		service.requestsCond.L.Unlock()
		service.requestsCond.Broadcast()
	}

	service.exchange.handlersChan <- struct {
		request        amqp.Publishing
		handleResponse func(amqp.Delivery)
		handleError    func(error)
	}{
		request: message,
		handleResponse: func(response amqp.Delivery) {
			if response.ContentType == "application/json" {
				handleSuccess(response.Body)
			} else {
				handleFail(errors.New(string(response.Body)))
			}
			reportHandling()
		},
		handleError: func(err error) {
			handleFail(err)
			reportHandling()
		}}

	err := service.exchange.channel.Publish(
		service.exchange.name, // exchange
		service.queue.Name,    // key
		true,                  // mandatory
		false,                 // immediate
		message)
	if err != nil {
		message.ContentType = "text/plain"
		response := amqp.Delivery{
			ContentType:   "text/plain",
			CorrelationId: message.CorrelationId,
			ReplyTo:       message.ReplyTo,
			Exchange:      service.exchange.name,
			Body: []byte(
				fmt.Sprintf(`Failed to publish RPC-request: "%s"`, err)),
		}
		service.client.LogError(string(response.Body) + ".")
		service.exchange.responseChan <- response
		return
	}

}

///////////////////////////////////////////////////////////////////////////////
