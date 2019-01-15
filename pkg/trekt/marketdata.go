// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/rektra-network/trekt-go/pkg/tradinglib"
	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// MarketDataExchange represents market data message exchange.
type MarketDataExchange struct{ mqExchange }

func createMarketDataExchange(
	trekt *Trekt, capacity uint16) (*MarketDataExchange, error) {

	result := &MarketDataExchange{}
	err := result.mqExchange.init("md", "topic", trekt, capacity)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the exchange.
func (exchange *MarketDataExchange) Close() {
	exchange.mqExchange.close()
}

// CreateServer creates a market data server to handle market data requests.
func (exchange *MarketDataExchange) CreateServer() (*MarketDataServer, error) {
	return createMarketDataServer(exchange)
}

// CreateServerOrExit creates an to handle market data requests or exits
// with error printing if creating is failed.
func (exchange *MarketDataExchange) CreateServerOrExit() *MarketDataServer {
	result, err := exchange.CreateServer()
	if err != nil {
		exchange.trekt.LogErrorf(`Failed to create market data server: "%s".`, err)
		os.Exit(1)
	}
	return result
}

// CreateService creates a market data service to receive market data.
func (exchange *MarketDataExchange) CreateService() (
	*MarketDataService, error) {

	return createMarketDataService(exchange)
}

///////////////////////////////////////////////////////////////////////////////

type marketDataStartRequest struct {
	SecurityID string
	IsStart    bool
}

///////////////////////////////////////////////////////////////////////////////

// MarketDataServer represents server which provides a market data by requests.
type MarketDataServer struct {
	mqRPCServer
}

func createMarketDataServer(
	exchange *MarketDataExchange) (*MarketDataServer, error) {

	result := &MarketDataServer{}
	err := result.mqRPCServer.init(
		exchange.trekt.Type+".control", &exchange.mqExchange)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the server.
func (server *MarketDataServer) Close() {
	server.mqRPCServer.close()
}

// Handle accepts market data requests and calls a handler for each.
func (server *MarketDataServer) Handle(
	handle func(securityID string, isStart bool) error) error {

	channel, err := server.exchange.trekt.mq.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	cancelledQueuesChan := channel.NotifyCancel(make(chan string))

	requestChan := make(chan struct {
		subscriber string
		request    marketDataStartRequest
	}, 1)
	go func() {
		server.handle(handle, requestChan)
		close(requestChan)
	}()

	subscribers := make(map[string]map[string]struct{})
	defer func() {
		if len(subscribers) == 0 {
			return
		}
		server.exchange.trekt.LogDebugf(
			"Stopping market data for %d securities"+
				" due to the handling process is stopped.",
			len(subscribers))
		for securityID := range subscribers {
			err := handle(securityID, false)
			if err != nil {
				server.exchange.trekt.LogErrorf(
					`Failed to stop market data for "%d"`+
						` by handling process is stopping: "%s".`,
					securityID, err)
			}
		}
	}()

	for {
		select {

		case request, isOpened := <-requestChan:
			if !isOpened {
				return nil
			}
			if _, isStarted := subscribers[request.request.SecurityID]; !isStarted {
				if !request.request.IsStart {
					break
				}
				subscribers[request.request.SecurityID] = make(map[string]struct{})
			} else if !request.request.IsStart {
				delete(subscribers[request.request.SecurityID], request.subscriber)
				break
			}
			subscribers[request.request.SecurityID][request.subscriber] = struct{}{}

		case canceledQueue := <-cancelledQueuesChan:
			for securityID := range subscribers {
				delete(subscribers[securityID], canceledQueue)
				if len(subscribers[securityID]) > 0 {
					continue
				}
				server.exchange.trekt.LogDebugf(
					`Stopping market data for "%d" by subscriber "%s" disconnection...`,
					securityID, canceledQueue)
				err := handle(securityID, false)
				if err != nil {
					server.exchange.trekt.LogErrorf(
						`Failed to stop market data for "%d"`+
							` by subscriber "%s" disconnection: "%s".`,
						securityID, canceledQueue, err)
				}
			}
		}
	}
}

func (server *MarketDataServer) handle(
	handle func(securityID string, isStart bool) error,
	requestChan chan struct {
		subscriber string
		request    marketDataStartRequest
	}) {

	server.mqRPCServer.handle(
		func(requestMessage amqp.Delivery) (interface{}, error) {

			request := marketDataStartRequest{}
			err := json.Unmarshal(requestMessage.Body, &request)
			if err != nil {
				server.exchange.trekt.LogErrorf(
					`Failed to parse market data request "%s" from "%s": "%s".`,
					string(requestMessage.Body), requestMessage.ReplyTo, err)
				return nil, errors.New("Internal error")
			}

			err = handle(request.SecurityID, request.IsStart)
			if err != nil {
				server.exchange.trekt.LogDebugf(
					`Failed to handle market data request "%s" from "%s": "%s".`,
					request, requestMessage.ReplyTo, err)
				return nil, err
			}

			{
				commandName := "started"
				if !request.IsStart {
					commandName = "stopped"
				}
				server.exchange.trekt.LogInfof(
					`Market data for security ID "%s" is %s by request from "%s".`,
					request.SecurityID, commandName, requestMessage.ReplyTo)
			}

			requestChan <- struct {
				subscriber string
				request    marketDataStartRequest
			}{subscriber: requestMessage.ReplyTo, request: request}

			return struct{}{}, nil
		})
}

///////////////////////////////////////////////////////////////////////////////

// MarketDataService represents service which accepts market data requests and
// provides market data requests by these requests.
type MarketDataService struct{ mqRPCClient }

func createMarketDataService(
	exchange *MarketDataExchange) (*MarketDataService, error) {
	result := &MarketDataService{}
	err := result.mqRPCClient.init(&exchange.mqExchange)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the service.
func (service *MarketDataService) Close() {
	service.mqRPCClient.close()
}

// Start requests a market data start.
func (service *MarketDataService) Start(
	security tradinglib.Security, handleFail func(error)) {
	service.request(security, true, handleFail)
}

// Stop requests a market data stop.
func (service *MarketDataService) Stop(
	security tradinglib.Security, handleFail func(error)) {
	service.request(security, false, handleFail)
}

func (service *MarketDataService) request(
	security tradinglib.Security, isStart bool, handleFail func(error)) {
	service.mqRPCClient.request(
		security.Exchange+".control", // routing key
		true,                         // mandatory
		marketDataStartRequest{SecurityID: security.ID, IsStart: isStart},
		func(response []byte) {}, // success handler
		handleFail)
}

///////////////////////////////////////////////////////////////////////////////
