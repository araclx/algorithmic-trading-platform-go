// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// MqHeartbeatServer is a message queuing heartbeat server.
type MqHeartbeatServer interface {
	Close()
	GetAddress() string
}

// MqHeartbeatTestFail describes heartbeat test fail.
type MqHeartbeatTestFail struct {
	Err     error
	Address string
}

// MqHeartbeatClient is a message queuing heartbeat client.
type MqHeartbeatClient interface {
	Close()
	GetFailedTestsChan() <-chan MqHeartbeatTestFail
	AddAddress(address string)
	RemoveAddress(address string)
	ReplaceAddress(map[string]interface{})
}

////////////////////////////////////////////////////////////////////////////////

type mqHeartbeatServer struct{ rpc mqRPCServer }

// CreateMqHeartbeatServer creates heartbeat server object instance.
func CreateMqHeartbeatServer(
	mq MqChannel, trekt Trekt) (MqHeartbeatServer, error) {

	result := &mqHeartbeatServer{}
	err := result.rpc.init("", mq, trekt)
	if err != nil {
		return nil, err
	}
	go result.rpc.handle(func(request amqp.Delivery) (interface{}, error) {
		return nil, nil
	})
	return result, nil
}

func (server *mqHeartbeatServer) Close() { server.rpc.close() }

func (server *mqHeartbeatServer) GetAddress() string {
	return server.rpc.getReplyName()
}

////////////////////////////////////////////////////////////////////////////////

type mqHeartbeatClientRequest struct {
	address *struct {
		address string
		isNew   bool
	}
	err *struct {
		address string
		err     error
	}
	replace map[string]interface{}
}

// CreateMqHeartbeatServer creates mqHeartbeatServer object instance.
type mqHeartbeatClient struct {
	failedTestsChan chan MqHeartbeatTestFail
	requestsChan    chan mqHeartbeatClientRequest
	stopWaiting     sync.WaitGroup
}

// CreateMqHeartbeatClient creates heartbeat server object instance.
func CreateMqHeartbeatClient(
	mq MqChannel, trekt Trekt) (MqHeartbeatClient, error) {

	rpc, err := createMqRPCClient(mq, trekt)
	if err != nil {
		return nil, err
	}

	result := &mqHeartbeatClient{
		failedTestsChan: make(chan MqHeartbeatTestFail),
		requestsChan:    make(chan mqHeartbeatClientRequest)}

	go func() {
		result.stopWaiting.Add(1)
		defer result.stopWaiting.Done()

		defer rpc.Close()

		ticker := trekt.CreateTicker(1 * time.Minute)
		defer ticker.Stop()

		addresses := map[string]interface{}{}

		for {
			select {

			case request, isOpened := <-result.requestsChan:
				if !isOpened {
					return
				}
				if request.replace != nil {
					addresses = request.replace
				}
				if request.err != nil {
					if _, has := addresses[request.err.address]; has {
						result.failedTestsChan <- MqHeartbeatTestFail{
							Err: request.err.err, Address: request.err.address}
						delete(addresses, request.err.address)
					}
				}
				if request.address != nil {
					if request.address.isNew {
						addresses[request.address.address] = nil
					} else {
						delete(addresses, request.address.address)
					}
				}

			case <-ticker.GetChan():
				for address := range addresses {
					disconnectedAddress := address
					rpc.Request(address, // routing key
						true,            // mandatory
						nil,             // request
						func([]byte) {}, // success handler
						func(err error) { // fail handler
							result.requestsChan <- mqHeartbeatClientRequest{
								err: &struct {
									address string
									err     error
								}{address: disconnectedAddress, err: err}}
						})
				}
			}
		}
	}()

	return result, nil
}

func (client *mqHeartbeatClient) Close() {
	close(client.requestsChan)
	client.stopWaiting.Wait()
	close(client.failedTestsChan)
}

func (client *mqHeartbeatClient) GetFailedTestsChan() <-chan MqHeartbeatTestFail {
	return client.failedTestsChan
}

func (client *mqHeartbeatClient) AddAddress(address string) {
	client.requestsChan <- mqHeartbeatClientRequest{
		address: &struct {
			address string
			isNew   bool
		}{address: address, isNew: true}}
}
func (client *mqHeartbeatClient) RemoveAddress(address string) {
	client.requestsChan <- mqHeartbeatClientRequest{
		address: &struct {
			address string
			isNew   bool
		}{address: address, isNew: false}}
}
func (client *mqHeartbeatClient) ReplaceAddress(list map[string]interface{}) {
	request := mqHeartbeatClientRequest{replace: map[string]interface{}{}}
	for address := range list {
		request.replace[address] = nil
	}
	client.requestsChan <- request
}

////////////////////////////////////////////////////////////////////////////////
