// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/rektra-network/trekt-go/pkg/mqclient"
	"golang.org/x/crypto/acme/autocert"
)

var (
	mqBroker    = flag.String("mq_broker", "localhost", "message queuing broker")
	name        = flag.String("name", "", "node instance name")
	host        = flag.String("host", "*:8443", "server host and port")
	isUnsecured = flag.Bool("unsecured", false,
		"do not use secure connections for client connection")
	endpoint = flag.String("endpoint", "/", "endpoint request path")
)

///////////////////////////////////////////////////////////////////////////////

func main() {
	flag.Parse()

	mq := mqclient.DealOrExit(*mqBroker, "scrtr-accesspoint", *name)
	defer mq.Close()

	upgrader := websocket.Upgrader{}

	var lastInstanceID uint64
	var numberOfConnections uint64

	router := mux.NewRouter()
	router.HandleFunc(
		*endpoint,
		func(writer http.ResponseWriter, request *http.Request) {
			mq.LogDebugf(`Opening connection from "%s" (%d)...`,
				request.RemoteAddr, atomic.AddUint64(&numberOfConnections, 1))
			conn, err := upgrader.Upgrade(writer, request, nil)
			if err != nil {
				mq.LogDebugf(
					`Failed to updgrade connection from "%s" (%d): "%s".`,
					request.RemoteAddr,
					atomic.AddUint64(&numberOfConnections, ^uint64(0)),
					err)
				return
			}
			defer conn.Close()
			user := createUser(conn, mq, atomic.AddUint64(&lastInstanceID, 1))
			defer func() {
				call := func(log func(format string, args ...interface{})) {
					log(`Closing connection from "%s" (%d)...`,
						request.RemoteAddr,
						atomic.AddUint64(&numberOfConnections, ^uint64(0)))
				}
				if user.login != nil {
					call(user.logInfof)
				} else {
					call(user.logDebugf)
				}
				user.close()
			}()
			user.run()
		})

	server := &http.Server{
		Handler: router,
	}

	mq.LogDebugf(`Opening server at "%s%s"...`, *host, *endpoint)
	var listener net.Listener
	if !*isUnsecured {
		listener = autocert.NewListener(*host)
	} else {
		var err error
		listener, err = net.Listen("tcp", *host)
		if err != nil {
			mq.LogErrorf(`Failed to start listener: "%s".`, err)
			return
		}
	}
	defer listener.Close()
	{
		secureType := "Secured"
		if *isUnsecured {
			secureType = "Unsecured"
		}
		mq.LogDebugf(`%s server opened at "%s".`,
			secureType,
			listener.Addr().String())
	}
	server.Serve(listener)
	mq.LogDebugf(`Server is stopped.`)
}

///////////////////////////////////////////////////////////////////////////////

func createUser(
	conn *websocket.Conn,
	mq *mqclient.Client,
	isntanceID uint64) *user {
	result := &user{
		conn:       conn,
		mq:         mq,
		isntanceID: isntanceID,
	}
	result.logDebugf(`Connected from "%s".`, conn.RemoteAddr())
	return result
}

type user struct {
	conn       *websocket.Conn
	mq         *mqclient.Client
	isntanceID uint64
	writeChan  chan string
	login      *string
	handlers   map[string]func(topic string, data interface{}) bool
}

func (user *user) close() {}

func (user *user) run() {
	user.handlers = map[string]func(string, interface{}) bool{
		"auth": user.auth,
	}

	readChan := make(chan []byte, 100)
	go func() {
		defer close(readChan)
		for {
			_, data, err := user.conn.ReadMessage()
			if err != nil {
				user.logDebugf(`Failed to read data from the server: "%s".`, err)
				break
			}
			if len(data) == 0 {
				user.logDebugf("EOF is received from the connection.")
				break
			}
			readChan <- data
		}
	}()

	user.writeChan = make(chan string, 100)
	defer close(user.writeChan)
	writeStopChan := make(chan struct{})
	go func() {
		defer close(writeStopChan)
		for {
			message, isOpened := <-user.writeChan
			if isOpened {
				break
			}
			err := user.conn.WriteMessage(
				websocket.TextMessage, []byte(message))
			if err != nil {
				user.logDebugf(`Failed to send message: "%s".`, err)
				break
			}
		}
	}()

	for {
		select {
		case data, isOpen := <-readChan:
			if !isOpen || !user.handleClientMessages(data) {
				return
			}
		case <-writeStopChan:
			return
		}
	}
}

func (user *user) send(message string) {
	user.writeChan <- message
}

func (user *user) handleClientMessages(data []byte) bool {
	var messages []struct {
		Topic string
		Data  interface{}
	}
	err := json.Unmarshal(data, &messages)
	if err != nil {
		user.logWarnf(`Received invalid data packet: "%s". Message: %s.`,
			err, string(data))
		return false
	}

	for _, message := range messages {
		handler, hasHandler := user.handlers[message.Topic]
		if !hasHandler {
			user.logWarnf(`Received message for unknown topic "%s": "%s".`,
				message.Topic, string(data))
			return false
		}
		if !handler(message.Topic, message.Data) {
			return false
		}
	}
	return true
}

func (user *user) auth(topic string, data interface{}) bool {
	delete(user.handlers, topic)
	auth := struct {
		Login        string
		PasswordHash string
	}{}
	err := mapstructure.Decode(data, &auth)
	if err != nil {
		user.logWarnf(`Received auth request with wrong format "%s": "%s".`,
			data, err)
		return false
	}
	if auth.Login != "guest" || auth.PasswordHash != "xXx" {
		user.logWarnf(`Credentials to auth are wrong: "%s".`, data)
		return false
	}
	user.login = &auth.Login
	user.logInfo("Successfully authenticated.")
	return true
}

func (user *user) formatLogRecord(source string) string {
	if user.login == nil {
		return fmt.Sprintf("%d: ", user.isntanceID) + source
	}
	return fmt.Sprintf(`"%s".%d: `, *user.login, user.isntanceID) + source
}
func (user *user) logErrorf(format string, args ...interface{}) {
	user.mq.LogErrorf(user.formatLogRecord(format), args...)
}
func (user *user) logError(message string) {
	user.mq.LogError(user.formatLogRecord(message))
}
func (user *user) logWarnf(format string, args ...interface{}) {
	user.mq.LogWarnf(user.formatLogRecord(format), args...)
}
func (user *user) logWarn(message string) {
	user.mq.LogWarn(user.formatLogRecord(message))
}
func (user *user) logInfof(format string, args ...interface{}) {
	user.mq.LogInfof(user.formatLogRecord(format), args...)
}
func (user *user) logInfo(message string) {
	user.mq.LogInfo(user.formatLogRecord(message))
}
func (user *user) logDebugf(format string, args ...interface{}) {
	user.mq.LogDebugf(user.formatLogRecord(format), args...)
}
func (user *user) logDebug(message string) {
	user.mq.LogDebug(user.formatLogRecord(message))
}

///////////////////////////////////////////////////////////////////////////////
