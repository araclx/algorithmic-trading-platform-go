// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/rektra-network/trekt-go/pkg/mqclient"

	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
)

type connection struct {
	conn            *websocket.Conn
	service         *service
	instanceID      uint64
	writeChan       chan string
	stateChangeChan chan func() bool
	auth            *mqclient.Auth
	isLogged        int32
	handlers        map[string]func(topic string, data interface{}) bool
}

func createConnection(
	conn *websocket.Conn,
	service *service,
	instanceID uint64) *connection {

	result := &connection{
		conn:       conn,
		service:    service,
		instanceID: instanceID,
	}
	result.logDebugf(`Connected from "%s".`, conn.RemoteAddr())
	return result
}

func (connection *connection) close() {}

func (connection *connection) run() {
	connection.handlers = map[string]func(string, interface{}) bool{
		"auth": connection.authorize,
	}

	readChan := make(chan []byte, 100)
	go func() {
		defer close(readChan)
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
			readChan <- data
		}
	}()

	connection.writeChan = make(chan string, 100)
	defer close(connection.writeChan)
	writeStopChan := make(chan struct{})
	go func() {
		defer close(writeStopChan)
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
	}()

	connection.stateChangeChan = make(chan func() bool, 100)
	defer close(connection.stateChangeChan)

	for {
		select {
		case data, isOpened := <-readChan:
			if !isOpened || !connection.handleClientMessages(data) {
				return
			}
		case changeState := <-connection.stateChangeChan:
			if !changeState() {
				return
			}
		case <-writeStopChan:
			return
		}
	}
}

func (connection *connection) send(topic, data string) {
	connection.writeChan <- `[{"topic":"` + topic + `","data":` + data + `}]`
}

func (connection *connection) handleClientMessages(data []byte) bool {
	var messages []struct {
		Topic string
		Data  interface{}
	}
	err := json.Unmarshal(data, &messages)
	if err != nil {
		connection.logWarnf(`Received invalid data packet: "%s". Message: %s.`,
			err, string(data))
		return false
	}

	for _, message := range messages {
		handler, hasHandler := connection.handlers[message.Topic]
		if !hasHandler {
			connection.logWarnf(`Received message for unknown topic "%s": "%s".`,
				message.Topic, string(data))
			return false
		}
		if !handler(message.Topic, message.Data) {
			return false
		}
	}
	return true
}

func (connection *connection) authorize(topic string, data interface{}) bool {
	delete(connection.handlers, topic)
	request := struct {
		Login    string
		Password string
	}{}
	err := mapstructure.Decode(data, &request)
	if err != nil {
		connection.logWarnf(
			`Received authorize-request with the wrong format "%s": "%s".`,
			data, err)
		return false
	}

	connection.logDebugf(`Authorizing with login "%s"...`, request.Login)
	connection.service.auth.Request(request.Login, request.Password,
		func(auth mqclient.Auth) {
			connection.stateChangeChan <- func() bool {
				connection.auth = &auth
				atomic.AddInt32(&connection.isLogged, 1)
				connection.logInfof(`Successfully authorized with login "%s".`,
					request.Login)
				connection.send(topic, "true")
				return true
			}
		},
		func(err error) {
			connection.logInfof(`Failed to authorize with login "%s": "%s".`,
				request.Login, err)
			connection.send(topic, "false")
			connection.stateChangeChan <- func() bool { return false }
		})

	return true
}

func (connection *connection) formatLogRecord(source string) string {
	if atomic.LoadInt32(&connection.isLogged) == 0 {
		return fmt.Sprintf("%d: ", connection.instanceID) + source
	}
	return fmt.Sprintf(`"%s".%d: `, connection.auth.Login, connection.instanceID) + source
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
