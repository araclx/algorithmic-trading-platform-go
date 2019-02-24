// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

type service struct {
	websocketUpgrader websocket.Upgrader

	trekt      trekt.Trekt
	auth       *trekt.AuthService
	securities *trekt.SecuritiesSubscription
	marketData *trekt.MarketDataService

	lastInstanceID      uint64
	numberOfConnections uint64
}

func (service *service) handle(
	writer http.ResponseWriter,
	request *http.Request) {

	service.trekt.LogDebugf(`Opening connection from "%s" (%d)...`,
		request.RemoteAddr, atomic.AddUint64(&service.numberOfConnections, 1))

	conn, err := service.websocketUpgrader.Upgrade(writer, request, nil)
	if err != nil {
		service.trekt.LogDebugf(
			`Failed to upgrade connection from "%s" (%d): "%s".`,
			request.RemoteAddr,
			atomic.AddUint64(&service.numberOfConnections, ^uint64(0)),
			err)
		return
	}

	connection := createConnection(
		conn, service, atomic.AddUint64(&service.lastInstanceID, 1))
	connection.run()

	log := connection.logDebugf
	if connection.isLogged != 0 {
		log = connection.logInfof
	}
	log(`Closing connection from "%s" (%d)...`,
		request.RemoteAddr,
		atomic.AddUint64(&service.numberOfConnections, ^uint64(0)))

	connection.close()
}
