// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/rektra-network/trekt-go/pkg/mqclient"
)

var (
	mqBroker        = flag.String("mq_broker", "localhost", "message queuing broker")
	name            = flag.String("name", "", "node instance name")
	logRequest      = flag.String("log", "", "log request")
	accessPointHost = flag.String(
		"host", "", "address of access point to connect and test it")
	accessPointPath = flag.String("endpoint", "/", "access point server path")
	isSecured       = flag.Bool("secured", true, "use SSL instead plain")
	printMessages   = flag.Bool("print_messages",
		false, "if set - each incoming message will be printed"+
			" to the standard logger")
)

///////////////////////////////////////////////////////////////////////////////

func createClient(url url.URL) *client {
	conn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Fatalf(`Failed to connect to the access point "%s": "%s".`,
			url.String(), err)
	}
	log.Printf(`Connected to the access point "%s".`, url.String())
	return &client{
		conn:     conn,
		dataChan: make(chan string, 10000),
	}
}

type client struct {
	conn     *websocket.Conn
	dataChan chan string
}

func (client *client) close() {
	if client.conn != nil {
		client.conn.Close()
	}
	close(client.dataChan)
}

func (client *client) run() {
	client.dataChan <- `[
		{
			"topic": "auth",
			"data": {
				"login": "guest",
				"passwordHash": "xXx"
			}
		}
	]`
	connCloseChan := make(chan struct{})
	go func() {
		defer close(connCloseChan)
		for {
			_, data, err := client.conn.ReadMessage()
			if err != nil {
				log.Printf(`Failed to read data from the server: "%s".`, err)
				return
			}
			if len(data) == 0 {
				log.Printf("EOF is received from the connection.")
				return
			}
			if *printMessages {
				fmt.Println("== Access point message begin: =================================================")
				fmt.Println(string(data))
				fmt.Println("== Access point message end. ===================================================")
			}
			var messages map[string]interface{}
			err = json.Unmarshal(data, &messages)
			if err != nil {
				log.Printf(
					`Failed to parase data from the connection: "%s".`, err)
				return
			}
			go client.handleServerMessage(messages)
		}
	}()

	defer log.Println("Connection closed.")
	for {
		select {
		case <-connCloseChan:
			return
		case message := <-client.dataChan:
			err := client.conn.WriteMessage(
				websocket.TextMessage, []byte(message))
			if err != nil {
				log.Printf(`Failed to send message: "%s".`, err)
				return
			}
		}
	}
}

func (client *client) handleServerMessage(messages map[string]interface{}) {
	for name, body := range messages {
		fmt.Println("==============")
		fmt.Println(name + ":")
		fmt.Printf("%v", body)
	}
}

///////////////////////////////////////////////////////////////////////////////

type app struct {
	mq              *mqclient.Client
	logSubscription *mqclient.Subscription
	client          *client
}

func (app *app) close() {
	if app.client != nil {
		app.client.close()
		app.client = nil
	}
	if app.logSubscription != nil {
		app.logSubscription.Close()
		app.logSubscription = nil
	}
	if app.mq != nil {
		app.mq.Close()
		app.mq = nil
	}
}

func (app *app) startLogListening(request string) {
	var err error
	app.logSubscription, err = app.mq.Subscribe(app.mq.Log, request)
	if err != nil {
		log.Fatalf(`Failed to subsctibe: "%s".`, err)
	}
	app.logSubscription.Handle(
		func(message mqclient.Message) {
			log.Printf("Log event: %s\t%s: %s",
				strings.ToUpper(message.GetTopic()),
				message.GetNodeID(),
				message.GetBody())
		})
}

func (app *app) startAccessPointReading(host, path string, isSecured bool) {
	url := url.URL{Scheme: "wss", Host: host, Path: path}
	if !isSecured {
		url.Scheme = "ws"
	}
	app.client = createClient(url)
	go app.client.run()
}

///////////////////////////////////////////////////////////////////////////////

func main() {
	flag.Parse()

	app := app{mq: mqclient.DealOrExit(*mqBroker, "cli", *name)}
	defer app.close()

	if *logRequest != "" {
		app.startLogListening(*logRequest)
	}
	if *accessPointHost != "" {
		app.startAccessPointReading(
			*accessPointHost, *accessPointPath, *isSecured)
	}

	log.Printf("To exit press CTRL+C")
	foreverChan := make(chan bool)
	<-foreverChan

}

///////////////////////////////////////////////////////////////////////////////
