// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"fmt"
	"sync/atomic"

	"github.com/rektra-network/trekt-go/pkg/mqclient"

	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
)

type connection struct {
	conn       *websocket.Conn
	service    *service
	instanceID uint64

	writeChan      chan string
	securitiesChan chan mqclient.SecurityStateList
	strandChan     chan func() bool

	protocol protocol

	isLogged int32
	user     user
}

func createConnection(
	conn *websocket.Conn,
	service *service,
	instanceID uint64) *connection {

	result := &connection{
		conn:       conn,
		service:    service,
		instanceID: instanceID,
		protocol:   createProtocol(),
		user:       createUser(mqclient.Auth{}),
	}
	result.logDebugf(`Connected from "%s".`, conn.RemoteAddr())
	return result
}

func (connection *connection) close() {
	connection.user.close()
	connection.protocol.close()
	connection.conn.Close()
}

func (connection *connection) run() {
	connection.user.methods = map[string]func(string, interface{}) bool{
		connection.protocol.authTopic(): connection.authorize,
	}

	connection.strandChan = make(chan func() bool, 1)
	defer close(connection.strandChan)

	readStopChan := make(chan struct{})
	go func() {
		for {
			_, data, err := connection.conn.ReadMessage()
			if err != nil {
				connection.logDebugf(`Failed to read data from the server: "%s".`, err)
				break
			}
			if len(data) == 0 {
				connection.logDebugf("EOF is received from the connection.")
				break
			}
			connection.strandChan <- func() bool {
				return connection.handleClientMessages(data)
			}
		}
		close(readStopChan)
	}()

	connection.writeChan = make(chan string, 1)
	defer close(connection.writeChan)
	writeStopChan := make(chan struct{})
	go func() {
		for {
			message, isOpened := <-connection.writeChan
			if !isOpened {
				break
			}
			err := connection.conn.WriteMessage(
				websocket.TextMessage, []byte(message))
			if err != nil {
				connection.logDebugf(`Failed to send message: "%s".`, err)
				break
			}
		}
		close(writeStopChan)
	}()

	defer func() {
		if connection.securitiesChan != nil {
			close(connection.securitiesChan)
		}
	}()

	for {
		select {
		case task := <-connection.strandChan:
			if !task() {
				return
			}
		case update, isOpened := <-connection.securitiesChan:
			if !isOpened {
				return
			}
			connection.send(connection.protocol.securityList(update))
		case <-readStopChan:
			return
		case <-writeStopChan:
			return
		}
	}
}

func (connection *connection) send(message message) {
	connection.writeChan <- message.export()
}

func (connection *connection) handleClientMessages(data []byte) bool {
	message, err := connection.protocol.parse(data)
	if err != nil {
		connection.logWarnf(
			`Received invalid (1) data packet: "%s". Message: %s.`,
			err, string(data))
		return false
	}
	if len(message) > 1 {
		connection.logWarnf(
			`Received invalid (2) data packet: "%s". Message: %s.`,
			err, string(data))
		return false
	}
	for topic, args := range message {
		handler, hasHandler := connection.user.methods[topic]
		if !hasHandler {
			connection.logWarnf(`Received message for unknown topic "%s": "%s".`,
				topic, string(data))
			return false
		}
		if !handler(topic, args) {
			return false
		}
	}
	return true
}

func (connection *connection) authorize(topic string, data interface{}) bool {
	delete(connection.user.methods, topic)
	request := mqclient.AuthRequest{}
	err := mapstructure.Decode(data, &request)
	if err != nil {
		connection.logWarnf(
			`Received authorize-request in the wrong format "%s": "%s".`,
			data, err)
		return false
	}

	connection.logDebugf(`Authorizing with login "%s"...`, request.Login)
	connection.service.auth.Request(
		request,
		func(auth mqclient.Auth) {
			connection.strandChan <- func() bool {
				if !connection.initUser(auth) {
					connection.send(connection.protocol.error("Internal error."))
					return false
				}
				atomic.AddInt32(&connection.isLogged, 1)
				connection.logInfof(`Successfully authorized with login "%s".`,
					request.Login)
				connection.send(connection.protocol.authSuccess())
				return true
			}
		},
		func(err error) {
			connection.logInfof(`Failed to authorize with login "%s": "%s".`,
				request.Login, err)
			connection.send(connection.protocol.authFail())
			connection.strandChan <- func() bool { return false }
		})

	return true
}

func (connection *connection) sendSecurityList() {
	connection.service.securities.Request(
		func(securities mqclient.SecurityStateList) {
			connection.strandChan <- func() bool {
				if connection.securitiesChan == nil {
					connection.securitiesChan = make(chan mqclient.SecurityStateList, 1)
					connection.service.securities.Notify(connection.securitiesChan)
				}
				connection.send(connection.protocol.securityList(securities))
				return true
			}
		})
}

func (connection *connection) initUser(auth mqclient.Auth) bool {
	user := createUser(auth)
	for key, value := range connection.user.methods {
		user.methods[key] = value
	}

	user.methods[connection.protocol.securityListTopic()] =
		func(string, interface{}) bool {
			connection.sendSecurityList()
			return true
		}

	if user.auth.IsMarketDataAllowed && !connection.initMarketMethods(&user) {
		return false
	}

	connection.user = user
	return true
}

func (connection *connection) initMarketMethods(user *user) bool {
	var err error
	user.marketData, err = connection.service.marketData.CreateService()
	if err != nil {
		connection.logErrorf(`Failed to create market data service: "%s".`, err)
		return false
	}
	return true
}

func (connection *connection) formatLogRecord(source string) string {
	if atomic.LoadInt32(&connection.isLogged) == 0 {
		return fmt.Sprintf("%d: ", connection.instanceID) + source
	}
	return fmt.Sprintf(`"%s".%d: `,
		connection.user.auth.Login,
		connection.instanceID) + source
}
func (connection *connection) logErrorf(format string, args ...interface{}) {
	connection.service.mq.LogErrorf(connection.formatLogRecord(format), args...)
}
func (connection *connection) logError(message string) {
	connection.service.mq.LogError(connection.formatLogRecord(message))
}
func (connection *connection) logWarnf(format string, args ...interface{}) {
	connection.service.mq.LogWarnf(connection.formatLogRecord(format), args...)
}
func (connection *connection) logWarn(message string) {
	connection.service.mq.LogWarn(connection.formatLogRecord(message))
}
func (connection *connection) logInfof(format string, args ...interface{}) {
	connection.service.mq.LogInfof(connection.formatLogRecord(format), args...)
}
func (connection *connection) logInfo(message string) {
	connection.service.mq.LogInfo(connection.formatLogRecord(message))
}
func (connection *connection) logDebugf(format string, args ...interface{}) {
	connection.service.mq.LogDebugf(connection.formatLogRecord(format), args...)
}
func (connection *connection) logDebug(message string) {
	connection.service.mq.LogDebug(connection.formatLogRecord(message))
}
