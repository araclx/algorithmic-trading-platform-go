// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/rektra-network/trekt-go/pkg/mqclient"
)

type service struct {
	mq       *mqclient.Client
	auth     *mqclient.AuthService
	upgrader websocket.Upgrader

	lastInstanceID      uint64
	numberOfConnections uint64
}

func (service *service) handle(
	writer http.ResponseWriter,
	request *http.Request) {

	service.mq.LogDebugf(`Opening connection from "%s" (%d)...`,
		request.RemoteAddr, atomic.AddUint64(&service.numberOfConnections, 1))

	conn, err := service.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		service.mq.LogDebugf(
			`Failed to updgrade connection from "%s" (%d): "%s".`,
			request.RemoteAddr,
			atomic.AddUint64(&service.numberOfConnections, ^uint64(0)),
			err)
		return
	}
	defer conn.Close()

	connection := createConnection(
		conn, service, atomic.AddUint64(&service.lastInstanceID, 1))
	defer func() {
		call := func(log func(format string, args ...interface{})) {
			log(`Closing connection from "%s" (%d)...`,
				request.RemoteAddr,
				atomic.AddUint64(&service.numberOfConnections, ^uint64(0)))
		}
		if connection.auth != nil {
			call(connection.logInfof)
		} else {
			call(connection.logDebugf)
		}
		connection.close()
	}()

	connection.run()
}
