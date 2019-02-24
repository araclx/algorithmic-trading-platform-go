// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"time"

	"github.com/gorilla/websocket"
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

func runStreamClient(
	exchange trekt.MarketDataExchange,
	stopChan <-chan interface{}) {

	requestsChan := make(chan struct {
		securityID string
		isStart    bool
	})
	defer close(requestsChan)

	server := exchange.CreateServerOrExit()
	defer server.Close()
	go server.Handle(func(security string, isStart bool) error {
		requestsChan <- struct {
			securityID string
			isStart    bool
		}{securityID: security, isStart: isStart}
		return nil
	})

	subscription := make(map[string]interface{})
	updateSubscription := func(request struct {
		securityID string
		isStart    bool
	}) {
		if request.isStart {
			subscription[request.securityID] = nil
		} else {
			delete(subscription, request.securityID)
		}
	}

	for {
		select {

		case request := <-requestsChan:
			updateSubscription(request)

			ticker := time.NewTicker(5 * time.Second)

			// It has to wait for some time if other requests will be sent
			// immediately after this to don't make too many requests when one client
			// subscribes many securities subsequently:
		requestsThinningLoop:
			for {
				select {
				case <-ticker.C:
					ticker.Stop()
					break requestsThinningLoop
				case request = <-requestsChan:
					updateSubscription(request)
				case <-stopChan:
					return
				}
			}

			{
				securities := make([]string, len(subscription))
				{
					i := 0
					for security := range subscription {
						securities[i] = security
						i++
					}
				}
				numberOfFailedConnections := 0
				for {
					client, isNotStopped := createStreamClientOrWait(
						securities, server, exchange.GetTrekt(),
						&numberOfFailedConnections, stopChan)
					if !isNotStopped {
						return
					}
					if client == nil {
						continue
					}
					defer client.close()
					isNotStopped = client.run(stopChan)
					if !isNotStopped {
						return
					}
				}
			}

		case <-stopChan:
			return

		}
	}
}

type streamClient struct {
	server *trekt.MarketDataServer
	trekt  trekt.Trekt
	conn   *websocket.Conn
}

func createStreamClientOrWait(
	securities []string,
	server *trekt.MarketDataServer,
	trekt trekt.Trekt,
	numberOfFailedConnections *int,
	stopChan <-chan interface{}) (*streamClient, bool) {

	result, err := createStreamClient(securities, server, trekt)
	if err == nil {
		trekt.LogInfof("Connected to the stream access point (%d).",
			*numberOfFailedConnections)
		*numberOfFailedConnections = 0
		return result, true
	}

	(*numberOfFailedConnections)++

	trekt.LogErrorf(
		`Failed to connect the stream access point: "%s" (%d).`,
		err, *numberOfFailedConnections)

	sleepTime := 5 * time.Second
	if *numberOfFailedConnections > 30 {
		sleepTime = 180 * time.Second
	} else if *numberOfFailedConnections > 15 {
		sleepTime = 60 * time.Second
	} else if *numberOfFailedConnections > 5 {
		sleepTime = 15 * time.Second
	}

	ticker := time.NewTicker(sleepTime)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		return nil, true
	case <-stopChan:
		return nil, false
	}
}

func createStreamClient(
	securities []string,
	server *trekt.MarketDataServer,
	trekt trekt.Trekt) (*streamClient, error) {

	url := "wss://stream.binance.com:9443/stream?streams="
	for i, security := range securities {
		if i != 0 {
			url += "/"
		}
		url += security + "@depth5"
	}

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	result := &streamClient{
		server: server,
		trekt:  trekt,
		conn:   conn}

	return result, nil
}

func (client *streamClient) close() {
	client.conn.Close()
	client.trekt.LogInfo("Connection closed.")
}

func (client *streamClient) run(stopChan <-chan interface{}) bool {

	readChan := make(chan []byte, 1)
	go func() {
		defer close(readChan)
		for {
			_, data, err := client.conn.ReadMessage()
			if err != nil {
				client.trekt.LogDebugf(`Failed to read data from the server: "%s".`,
					err)
				return
			}
			if len(data) == 0 {
				client.trekt.LogDebugf("EOF is received from the connection.")
				return
			}
			readChan <- data
		}
	}()

	for {
		select {
		case data, isOpened := <-readChan:
			if !isOpened {
				return true
			}
			client.handleMessages(data)
		case <-stopChan:
			return false
		}
	}
}

func (client *streamClient) handleMessages(data []byte) {
	client.trekt.LogDebugf("MESSAGE: %s", string(data))
}
