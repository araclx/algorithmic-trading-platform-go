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

type rpc struct {
	exchange     *exchange
	trekt        *Trekt
	subscription clientSubscription
}

func (rpc *rpc) init(query string, exchange *exchange, trekt *Trekt) error {
	rpc.exchange = exchange
	rpc.trekt = trekt
	return rpc.subscription.init(
		query,
		rpc.exchange,
		true, // is auto-ack
		trekt)
}

func (rpc *rpc) close() {
	rpc.subscription.close()
}

func (rpc *rpc) getReplyName() string {
	return rpc.subscription.queue.Name
}

///////////////////////////////////////////////////////////////////////////////

type rpcServer struct{ rpc }

func (server *rpcServer) init(
	subscriptionQuery string, exchange *exchange, trekt *Trekt) error {

	return server.rpc.init(subscriptionQuery, exchange, trekt)
}

func (server *rpcServer) close() {
	server.rpc.close()
}

func (server *rpcServer) handle(
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
				server.trekt.LogErrorf(
					`Failed to publish RPC-server response: "%s".`, err)
			}
		})
}

///////////////////////////////////////////////////////////////////////////////

type rpcClient struct {
	rpc
	requestsCond *sync.Cond
	requests     map[string]amqp.Publishing
}

func createRPCClient(
	exchange *exchange, trekt *Trekt) (*rpcClient, error) {

	result := &rpcClient{}
	err := result.init(exchange, trekt)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (trekt *rpcClient) init(exchange *exchange, mqClient *Trekt) error {

	err := trekt.rpc.init("", exchange, mqClient)
	if err != nil {
		return err
	}

	trekt.requestsCond = sync.NewCond(&sync.Mutex{})
	trekt.requests = make(map[string]amqp.Publishing)

	go trekt.subscription.handle(func(response amqp.Delivery) {
		trekt.exchange.responseChan <- response
	})

	return nil
}

func (trekt *rpcClient) close() {
	trekt.requestsCond.L.Lock()
	for _, request := range trekt.requests {
		trekt.exchange.responseChan <- amqp.Delivery{
			ContentType:   "text/plain",
			CorrelationId: request.CorrelationId,
			ReplyTo:       request.ReplyTo,
			Exchange:      trekt.exchange.name,
			Body: []byte(
				"RPC-service is stopped, request result is unknown"),
		}
	}
	for len(trekt.requests) > 0 {
		trekt.requestsCond.Wait()
	}
	trekt.requestsCond.L.Unlock()

	trekt.rpc.close()
}

func (trekt *rpcClient) request(
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
		ReplyTo:       trekt.getReplyName(),
		ContentType:   "application/json",
		Body:          requestData}

	trekt.requestsCond.L.Lock()
	trekt.requests[message.CorrelationId] = message
	trekt.requestsCond.L.Unlock()
	reportHandling := func() {
		trekt.requestsCond.L.Lock()
		delete(trekt.requests, message.CorrelationId)
		trekt.requestsCond.L.Unlock()
		trekt.requestsCond.Broadcast()
	}

	trekt.exchange.handlersChan <- struct {
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

	err = trekt.exchange.publish(
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
			Exchange:      trekt.exchange.name,
			Body: []byte(fmt.Sprintf(`Failed to publish RPC-request: "%s"`,
				err)),
		}
		trekt.trekt.LogError(string(response.Body) + ".")
		trekt.exchange.responseChan <- response
		return
	}

}

///////////////////////////////////////////////////////////////////////////////
