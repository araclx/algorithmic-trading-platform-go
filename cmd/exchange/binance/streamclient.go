// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"sync"
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

	clientStopsChan := make(chan interface{})
	defer close(clientStopsChan)

	var client *streamClient
	closeClient := func() {
		if client != nil {
			clientCopy := client
			client = nil
			clientCopy.close()
		}
	}
	defer closeClient()

	var lastConnectTime time.Time
	reconnect := func() {
		closeClient()
		if len(subscription) == 0 {
			return
		}
		numberOfFailedConnections := 0
		for client == nil {
			var isNotStopped bool
			client, isNotStopped = createStreamClientOrWait(
				subscription, server, exchange.GetTrekt(),
				&numberOfFailedConnections, lastConnectTime, stopChan)
			if !isNotStopped {
				return
			}
		}
		lastConnectTime = time.Now()
		go func() {
			defer func() {
				if client != nil {
					clientStopsChan <- nil
				}
			}()
			client.run()
		}()
	}

	for {
		select {
		case request := <-requestsChan:
			updateSubscription(request)
			ticker := time.NewTicker(5 * time.Second)
			// It has to wait for some time if other requests will be sent immediately
			// after this to don't make too many requests when one client  subscribes
			// many securities subsequently:
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
			reconnect()
		case <-clientStopsChan:
			reconnect()
		case <-stopChan:
			return
		}
	}
}

type streamClient struct {
	server      *trekt.MarketDataServer
	trekt       trekt.Trekt
	conn        *websocket.Conn
	stopWaiting sync.WaitGroup
}

func createStreamClientOrWait(
	subscription map[string]interface{},
	server *trekt.MarketDataServer,
	trekt trekt.Trekt,
	numberOfFailedConnections *int,
	lastConnectTime time.Time,
	stopChan <-chan interface{}) (*streamClient, bool) {

	liveTime := time.Now().Sub(lastConnectTime)
	if liveTime > 15*time.Second {
		result, err := createStreamClient(subscription, server, trekt)
		if err == nil {
			trekt.LogInfof(
				"Connected to the stream (number of failed attempts: %d).",
				*numberOfFailedConnections)
			*numberOfFailedConnections = 0
			return result, true
		}
		(*numberOfFailedConnections)++
		trekt.LogErrorf(`Failed to connect the stream: "%s" (%d).`,
			err, *numberOfFailedConnections)
	}

	sleepTime := 5 * time.Second
	if *numberOfFailedConnections > 30 {
		sleepTime = 180 * time.Second
	} else if *numberOfFailedConnections > 15 || liveTime < 5*time.Second {
		sleepTime = 60 * time.Second
	} else if *numberOfFailedConnections > 5 || liveTime < 15*time.Second {
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
	subscription map[string]interface{},
	server *trekt.MarketDataServer,
	trekt trekt.Trekt) (*streamClient, error) {

	url := "wss://stream.binance.com:9443/stream?streams="
	{
		isStart := true
		for symbol := range subscription {
			if isStart {
				isStart = false
			} else {
				url += "/"
			}
			url += symbol + "@depth5"
		}
	}
	trekt.LogDebugf(`Connecting to the stream "%s"...`, url)

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
	client.stopWaiting.Wait()
	client.trekt.LogInfo("Connection closed.")
}

func (client *streamClient) run() {
	client.stopWaiting.Add(1)
	defer client.stopWaiting.Done()
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
		client.handleMessages(data)
	}
}

func (client *streamClient) handleMessages(data []byte) {
	client.trekt.LogDebugf("MESSAGE: %s", string(data))
}
