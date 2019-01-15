// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

type mqRPC struct {
	exchange     *mqExchange
	subscription mqSubscription
}

func (rpc *mqRPC) init(query string, exchange *mqExchange) error {
	rpc.exchange = exchange
	return rpc.subscription.init(
		query,
		rpc.exchange,
		true) // is auto-ack
}

func (rpc *mqRPC) close() {
	rpc.subscription.close()
}

func (rpc *mqRPC) getReplyName() string {
	return rpc.subscription.queue.Name
}

///////////////////////////////////////////////////////////////////////////////

type mqRPCServer struct{ mqRPC }

func (server *mqRPCServer) init(
	subscriptionQuery string, exchange *mqExchange) error {

	return server.mqRPC.init(subscriptionQuery, exchange)
}

func (server *mqRPCServer) close() {
	server.mqRPC.close()
}

func (server *mqRPCServer) handle(
	handle func(amqp.Delivery) (response interface{}, err error)) {

	server.subscription.handle(
		func(request amqp.Delivery) {
			response, err := handle(request)
			message := amqp.Publishing{CorrelationId: request.CorrelationId}
			if err == nil {
				message.Body, err = json.Marshal(response)
			}
			if err != nil {
				message.ContentType = "text/plain"
				message.Body = []byte(err.Error())
			} else {
				message.ContentType = "application/json"
			}
			err = server.exchange.publish(
				request.ReplyTo, // routing key
				false,           // mandatory
				false,           // immediate
				message)
			if err != nil {
				server.exchange.trekt.LogErrorf(
					`Failed to publish RPC-server response: "%s".`, err)
			}
		})
}

///////////////////////////////////////////////////////////////////////////////

type mqRPCClient struct {
	mqRPC
	requestsCond *sync.Cond
	requests     map[string]amqp.Publishing
}

func createMqRPCClient(exchange *mqExchange) (*mqRPCClient, error) {
	result := &mqRPCClient{}
	err := result.init(exchange)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (client *mqRPCClient) init(exchange *mqExchange) error {

	err := client.mqRPC.init("", exchange)
	if err != nil {
		return err
	}

	client.requestsCond = sync.NewCond(&sync.Mutex{})
	client.requests = make(map[string]amqp.Publishing)

	go client.subscription.handle(func(response amqp.Delivery) {
		client.exchange.responseChan <- response
	})

	return nil
}

func (client *mqRPCClient) close() {
	client.requestsCond.L.Lock()
	for _, request := range client.requests {
		client.exchange.responseChan <- amqp.Delivery{
			ContentType:   "text/plain",
			CorrelationId: request.CorrelationId,
			ReplyTo:       request.ReplyTo,
			Exchange:      client.exchange.name,
			Body: []byte(
				"RPC-service is stopped, request result is unknown"),
		}
	}
	for len(client.requests) > 0 {
		client.requestsCond.Wait()
	}
	client.requestsCond.L.Unlock()

	client.mqRPC.close()
}

func (client *mqRPCClient) request(
	routingKey string,
	mandatory bool,
	request interface{},
	handleSuccess func([]byte),
	handleFail func(error)) {

	requestData, err := json.Marshal(request)
	if err != nil {
		handleFail(fmt.Errorf(`Failed to serialize RPC-request "%s": "%s"`,
			request, err))
		return
	}

	message := amqp.Publishing{
		CorrelationId: "1234",
		ReplyTo:       client.getReplyName(),
		ContentType:   "application/json",
		Body:          requestData}

	client.requestsCond.L.Lock()
	client.requests[message.CorrelationId] = message
	client.requestsCond.L.Unlock()
	reportHandling := func() {
		client.requestsCond.L.Lock()
		delete(client.requests, message.CorrelationId)
		client.requestsCond.L.Unlock()
		client.requestsCond.Broadcast()
	}

	client.exchange.handlersChan <- struct {
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

	err = client.exchange.publish(
		routingKey, // routing key
		mandatory,  // mandatory
		false,      // immediate
		message)
	if err != nil {
		message.ContentType = "text/plain"
		response := amqp.Delivery{
			ContentType:   "text/plain",
			CorrelationId: message.CorrelationId,
			ReplyTo:       message.ReplyTo,
			Exchange:      client.exchange.name,
			Body: []byte(fmt.Sprintf(`Failed to publish RPC-request: "%s"`,
				err)),
		}
		client.exchange.trekt.LogError(string(response.Body) + ".")
		client.exchange.responseChan <- response
	}

}

///////////////////////////////////////////////////////////////////////////////
