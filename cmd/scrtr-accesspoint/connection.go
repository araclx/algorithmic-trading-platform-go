// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rektra-network/trekt-go/pkg/tradinglib"

	"github.com/gorilla/websocket"
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

type connection struct {
	conn       *websocket.Conn
	service    *service
	instanceID uint64

	strandChan chan func() bool
	writeChan  chan string

	securitiesSubscription struct {
		id          trekt.SecuritiesSubscriptionNotificationID
		updatesChan <-chan trekt.SecurityStateList
		securities  map[string]*map[string]tradinglib.Security
	}
	depthOfMarketUpdatesChan chan trekt.DepthOfMarketUpdate

	protocol Protocol

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
		protocol:   CreateProtocol()}
	result.logDebugf(`Connected from "%s".`, conn.RemoteAddr())
	return result
}

func (connection *connection) close() {
	connection.protocol.Close()
	if connection.conn != nil {
		connection.conn.Close()
	}
}

func (connection *connection) run() {
	connection.strandChan = make(chan func() bool, 1)
	defer close(connection.strandChan)

	stopChan := make(chan interface{}, 1)
	defer close(stopChan)

	stopBarrier := sync.WaitGroup{}
	defer stopBarrier.Wait()

	defer func() {
		connection.conn.Close()
		connection.conn = nil
	}()
	stopBarrier.Add(1)
	go func() {
		defer func() {
			stopChan <- nil
			stopBarrier.Done()
		}()
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
	}()

	connection.writeChan = make(chan string, 1)
	defer close(connection.writeChan)
	stopBarrier.Add(1)
	go func() {
		defer func() {
			stopChan <- nil
			stopBarrier.Done()
		}()
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
		if connection.depthOfMarketUpdatesChan != nil {
			close(connection.depthOfMarketUpdatesChan)
		}
	}()

	connection.securitiesSubscription.securities =
		map[string]*map[string]tradinglib.Security{}

	connection.user = createUser(trekt.Auth{})
	defer connection.user.close()
	connection.user.methods[connection.protocol.authTopic()] =
		connection.authorize

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
			connection.updateSecurities(update)
		case update, isOpened := <-connection.depthOfMarketUpdatesChan:
			if !isOpened {
				return
			}
			connection.send(connection.protocol.DepthOfMarket(update))
		case <-stopChan:
			return
		}
	}
}

func (connection *connection) send(message Message) {
	connection.writeChan <- message.Export()
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

func (connection *connection) initUser(auth trekt.Auth) bool {
	user := createUser(auth)
	for key, value := range connection.user.methods {
		user.methods[key] = value
	}

	user.methods[connection.protocol.securityListTopic()] =
		connection.sendSecurityList

	if user.auth.IsMarketDataAllowed {
		user.methods[connection.protocol.depthOfMarketTopic()] =
			connection.startDepthOfMarket
	}

	connection.user.close()
	connection.user = user
	return true
}

func (connection *connection) updateSecurities(
	update trekt.SecurityStateList) {

	exchanges := map[string]*map[string]tradinglib.Security{}
	for _, security := range update {
		if security.IsActive != nil || !*security.IsActive {
			continue
		}
		securities, has := exchanges[security.Security.Exchange]
		if !has {
			newList := map[string]tradinglib.Security{}
			securities = &newList
		}
		(*securities)[security.Security.ID] = security.Security
	}
	connection.securitiesSubscription.securities = exchanges
	connection.send(connection.protocol.securityList(update))
}

func (connection *connection) resolveSecurity(
	exchange, security string) *tradinglib.Security {

	if list, has := connection.securitiesSubscription.securities[exchange]; has {
		if item, has := (*list)[security]; has {
			return &item
		}
	}
	return nil
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
