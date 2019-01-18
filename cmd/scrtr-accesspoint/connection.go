// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"fmt"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

type connection struct {
	conn       *websocket.Conn
	service    *service
	instanceID uint64

	writeChan              chan string
	securitiesSubscription struct {
		id          trekt.SecuritiesSubscriptionNotificationID
		updatesChan <-chan trekt.SecurityStateList
	}

	readStopChan chan struct{}
	strandChan   chan func() bool

	protocol protocol

	isLogged int32
	user     user
}

func createConnection(
	conn *websocket.Conn,
	service *service,
	instanceID uint64) *connection {

	result := &connection{
		conn:         conn,
		service:      service,
		instanceID:   instanceID,
		protocol:     createProtocol(),
		user:         createUser(trekt.Auth{}),
		readStopChan: make(chan struct{}),
		strandChan:   make(chan func() bool, 1)}

	go func() {
		defer close(result.readStopChan)
		for {
			_, data, err := result.conn.ReadMessage()
			if err != nil {
				result.logDebugf(`Failed to read data from the server: "%s".`, err)
				break
			}
			if len(data) == 0 {
				result.logDebugf("EOF is received from the connection.")
				break
			}
			result.strandChan <- func() bool {
				return result.handleClientMessages(data)
			}
		}
	}()

	result.logDebugf(`Connected from "%s".`, conn.RemoteAddr())
	return result
}

func (connection *connection) close() {
	connection.user.close()
	connection.protocol.close()
	connection.conn.Close()
	<-connection.readStopChan
	close(connection.strandChan)
}

func (connection *connection) run() {
	connection.user.methods = map[string]func(string, interface{}) bool{
		connection.protocol.authTopic(): connection.authorize,
	}

	connection.writeChan = make(chan string, 1)
	writeStopChan := make(chan struct{})
	defer func() {
		close(connection.writeChan)
		<-writeStopChan
	}()
	go func() {
		defer close(writeStopChan)
		for {
			message, isOpened := <-connection.writeChan
			if !isOpened {
				return
			}
			err := connection.conn.WriteMessage(
				websocket.TextMessage, []byte(message))
			if err != nil {
				connection.logDebugf(`Failed to send message: "%s".`, err)
				return
			}
		}
	}()

	defer func() {
		if connection.securitiesSubscription.updatesChan != nil {
			connection.service.securities.CloseNotification(
				connection.securitiesSubscription.id)
		}
	}()

	for {
		select {
		case task := <-connection.strandChan:
			if !task() {
				return
			}
		case update, isOpened := <-connection.securitiesSubscription.updatesChan:
			if !isOpened {
				return
			}
			connection.send(connection.protocol.securityList(update))
		case <-connection.readStopChan:
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
	request := trekt.AuthRequest{}
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
		func(auth trekt.Auth) {
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
		func(securities trekt.SecurityStateList) {
			connection.strandChan <- func() bool {
				if connection.securitiesSubscription.updatesChan == nil {
					connection.securitiesSubscription.id,
						connection.securitiesSubscription.updatesChan =
						connection.service.securities.CreateNotification()
				}
				connection.send(connection.protocol.securityList(securities))
				return true
			}
		})
}

func (connection *connection) initUser(auth trekt.Auth) bool {
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
	connection.service.trekt.LogErrorf(
		connection.formatLogRecord(format), args...)
}
func (connection *connection) logError(message string) {
	connection.service.trekt.LogError(connection.formatLogRecord(message))
}
func (connection *connection) logWarnf(format string, args ...interface{}) {
	connection.service.trekt.LogWarnf(connection.formatLogRecord(format), args...)
}
func (connection *connection) logWarn(message string) {
	connection.service.trekt.LogWarn(connection.formatLogRecord(message))
}
func (connection *connection) logInfof(format string, args ...interface{}) {
	connection.service.trekt.LogInfof(connection.formatLogRecord(format), args...)
}
func (connection *connection) logInfo(message string) {
	connection.service.trekt.LogInfo(connection.formatLogRecord(message))
}
func (connection *connection) logDebugf(format string, args ...interface{}) {
	connection.service.trekt.LogDebugf(
		connection.formatLogRecord(format), args...)
}
func (connection *connection) logDebug(message string) {
	connection.service.trekt.LogDebug(connection.formatLogRecord(message))
}
