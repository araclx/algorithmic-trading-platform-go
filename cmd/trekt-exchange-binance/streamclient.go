// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"net/url"
	"os"
	"time"

	"github.com/rektra-network/trekt-go/pkg/mqclient"

	"github.com/gorilla/websocket"
)

func runStreamClient(
	exchange *mqclient.MarketDataExchange,
	mq *mqclient.Client,
	stopChan <-chan struct{}) {

	requestsChan := make(chan struct {
		securityID string
		isStart    bool
	})
	defer close(requestsChan)

	server := exchange.CreateServerOrExit()
	defer server.Close()
	err := server.Handle(func(securityID string, isStart bool) error {
		requestsChan <- struct {
			securityID string
			isStart    bool
		}{securityID: securityID,
			isStart: isStart}
		return nil
	})
	if err != nil {
		mq.LogErrorf(`Failed to handle market data requests: "%s".`, err)
		os.Exit(1)
	}

	var clientStopChan chan struct{}
	defer func() {
		if clientStopChan != nil {
			clientStopChan <- struct{}{}
			close(clientStopChan)
		}
	}()

	subscription := make(map[string]struct{})
	updateSubscription := func(request struct {
		securityID string
		isStart    bool
	}) {
		if request.isStart {
			subscription[request.securityID] = struct{}{}
		} else {
			delete(subscription, request.securityID)
		}
	}

	for {
		select {

		case request := <-requestsChan:
			updateSubscription(request)

			ticker := time.NewTicker(5 * time.Second)
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

			if clientStopChan == nil {
				clientStopChan = make(chan struct{})
			} else {
				clientStopChan <- struct{}{}
			}
			go func() {
				defer func() { clientStopChan <- struct{}{} }()
				securities := make(map[string]struct{})
				numberOfFailedConnections := 0
				for {
					client, isNotStopped := createStreamClientOrWait(
						securities, server, mq, &numberOfFailedConnections, stopChan)
					if !isNotStopped {
						return
					}
					if client == nil {
						continue
					}
					isNotStopped = client.run(clientStopChan)
					client.close()
					if !isNotStopped {
						return
					}
				}
			}()

		case <-stopChan:
			return

		}
	}
}

type streamClient struct {
	server *mqclient.MarketDataServer
	mq     *mqclient.Client
	conn   *websocket.Conn

	writeChan chan string
}

func createStreamClientOrWait(
	securities map[string]struct{},
	server *mqclient.MarketDataServer,
	mq *mqclient.Client,
	numberOfFailedConnections *int,
	stopChan <-chan struct{}) (*streamClient, bool) {

	result, err := createStreamClient(securities, server, mq)
	if err == nil {
		mq.LogInfof("Connected to the stream access point (%d).",
			*numberOfFailedConnections)
		*numberOfFailedConnections = 0
		return result, true
	}

	(*numberOfFailedConnections)++

	mq.LogErrorf(
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
	securities map[string]struct{},
	server *mqclient.MarketDataServer,
	mq *mqclient.Client) (*streamClient, error) {

	url := url.URL{Scheme: "wss", Host: "stream.binance.com:9443", Path: "/"}
	conn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		return nil, err
	}

	result := &streamClient{
		server: server,
		conn:   conn,
	}

	return result, nil
}

func (client *streamClient) close() {
	client.conn.Close()
	client.mq.LogInfo("Connection closed.")
}

func (client *streamClient) run(stopChan <-chan struct{}) bool {

	readChan := make(chan []byte, 100)
	go func() {
		for {
			_, data, err := client.conn.ReadMessage()
			if err != nil {
				client.mq.LogDebugf(`Failed to read data from the server: "%s".`, err)
				break
			}
			if len(data) == 0 {
				client.mq.LogDebugf("EOF is received from the connection.")
				break
			}
			readChan <- data
		}
		close(readChan)
	}()

	client.writeChan = make(chan string, 100)
	defer close(client.writeChan)
	writeStopChan := make(chan struct{})
	go func() {
		for {
			message, isOpened := <-client.writeChan
			if !isOpened {
				break
			}
			err := client.conn.WriteMessage(
				websocket.TextMessage, []byte(message))
			if err != nil {
				client.mq.LogDebugf(`Failed to send message: "%s".`, err)
				break
			}
		}
		close(writeStopChan)
	}()

	for {
		select {
		case data, isOpened := <-readChan:
			if !isOpened {
				return true
			}
			client.handleMessages(data)
		case <-writeStopChan:
			return true
		case <-stopChan:
			return false
		}
	}
}

func (client *streamClient) handleMessages(data []byte) {
	client.mq.LogDebugf("MESSAGE: %s", string(data))
}
