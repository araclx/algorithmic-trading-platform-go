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
	trekt        Trekt
	channel      MqChannel
	subscription mqSubscription
}

func (rpc *mqRPC) init(query string, channel MqChannel, trekt Trekt) error {
	rpc.trekt = trekt
	rpc.channel = channel
	return rpc.subscription.init(
		query,
		rpc.channel,
		true, // is auto-ack
		rpc.trekt)
}

func (rpc *mqRPC) close() {
	rpc.subscription.close()
}

func (rpc *mqRPC) getReplyName() string {
	return rpc.subscription.queue.Name
}

///////////////////////////////////////////////////////////////////////////////

type mqRPCServer struct {
	mqRPC
}

func (server *mqRPCServer) init(
	subscriptionQuery string, channel MqChannel, trekt Trekt) error {

	return server.mqRPC.init(subscriptionQuery, channel, trekt)
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
			err = server.channel.Publish(
				request.ReplyTo, // routing key
				false,           // mandatory
				false,           // immediate
				message)
			if err != nil {
				server.trekt.LogErrorf(`Failed to publish RPC-server response: "%s".`,
					err)
			}
		})
}

///////////////////////////////////////////////////////////////////////////////

type mqRPCClient struct {
	mqRPC
	trekt        Trekt
	requestsCond *sync.Cond
	requests     map[string]amqp.Publishing
}

func createMqRPCClient(channel MqChannel, trekt Trekt) (RPCClient, error) {
	result := &mqRPCClient{trekt: trekt}
	err := result.init(channel, result.trekt)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (client *mqRPCClient) init(channel MqChannel, trekt Trekt) error {

	err := client.mqRPC.init("", channel, trekt)
	if err != nil {
		return err
	}

	client.requestsCond = sync.NewCond(&sync.Mutex{})
	client.requests = make(map[string]amqp.Publishing)

	go client.subscription.handle(func(response amqp.Delivery) {
		client.channel.Respond(response)
	})

	return nil
}

func (client *mqRPCClient) Close() {
	client.requestsCond.L.Lock()
	for _, request := range client.requests {
		client.channel.Respond(amqp.Delivery{
			ContentType:   "text/plain",
			CorrelationId: request.CorrelationId,
			ReplyTo:       request.ReplyTo,
			Body: []byte(
				"RPC-service is stopped, request result is unknown"),
		})
	}
	for len(client.requests) > 0 {
		client.requestsCond.Wait()
	}
	client.requestsCond.L.Unlock()

	client.mqRPC.close()
}

func (client *mqRPCClient) Request(
	routingKey string,
	mandatory bool,
	request interface{},
	handleSuccess func([]byte),
	handleFail func(error)) {

	message := amqp.Publishing{
		CorrelationId: "1234",
		ReplyTo:       client.getReplyName(),
		ContentType:   "application/json"}
	if request != nil {
		var err error
		message.Body, err = json.Marshal(request)
		if err != nil {
			handleFail(fmt.Errorf(`Failed to serialize RPC-request "%s": "%s"`,
				request, err))
			return
		}
	}

	client.requestsCond.L.Lock()
	client.requests[message.CorrelationId] = message
	client.requestsCond.L.Unlock()
	reportHandling := func() {
		client.requestsCond.L.Lock()
		delete(client.requests, message.CorrelationId)
		client.requestsCond.L.Unlock()
		client.requestsCond.Broadcast()
	}

	client.channel.RegisterHandlers(
		message, // request
		func(response amqp.Delivery) { // handleResponse
			if response.ContentType == "application/json" {
				handleSuccess(response.Body)
			} else {
				handleFail(errors.New(string(response.Body)))
			}
			reportHandling()
		},
		func(err error) { // handleError
			handleFail(err)
			reportHandling()
		})

	err := client.channel.Publish(
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
			Body: []byte(fmt.Sprintf(`Failed to publish RPC-request: "%s"`,
				err)),
		}
		client.trekt.LogError(string(response.Body) + ".")
		client.channel.Respond(response)
	}

}

///////////////////////////////////////////////////////////////////////////////
