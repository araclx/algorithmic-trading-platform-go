// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

type client struct {
	conn     *websocket.Conn
	dataChan chan string
}

func createClient(url url.URL) *client {
	conn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Fatalf(`Failed to connect to the access point "%s": "%s".`,
			url.String(), err)
	}
	log.Printf(`Connected to the access point "%s".`, url.String())
	return &client{
		conn:     conn,
		dataChan: make(chan string, 1),
	}
}

func (client *client) close() {
	close(client.dataChan)
	client.conn.Close()
}

func (client *client) run() {
	client.dataChan <- `{
			"auth": {
				"login": "guest",
				"password": "guest"
			}
		}`
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
				fmt.Println(
					"== Access point message begin: =================================================")
				dataString := string(data)
				maxLen := 255
				if len(dataString) > maxLen {
					fmt.Printf("%s...(%d bytes)\n", dataString[:maxLen], len(dataString))
				} else {
					fmt.Println(dataString)
				}
				fmt.Println(
					"== Access point message end. ===================================================")
			}
			var messages []map[string]interface{}
			err = json.Unmarshal(data, &messages)
			if err != nil {
				log.Printf(
					`Failed to parse data from the connection: "%s".`, err)
				return
			}
			for _, message := range messages {
				for topic, data := range message {
					client.handleServerTopic(topic, data)
				}
			}
		}
	}()

	defer log.Println("Connection closed.")
	for {
		select {
		case <-connCloseChan:
			return
		case message, isOpened := <-client.dataChan:
			if !isOpened {
				return
			}
			err := client.conn.WriteMessage(
				websocket.TextMessage, []byte(message))
			if err != nil {
				log.Printf(`Failed to send message: "%s".`, err)
				return
			}
		}
	}
}

func (client *client) handleServerTopic(topic string, data interface{}) {
	switch topic {

	case "auth":
		log.Println("Authorized, requesting securities info...")
		client.dataChan <- `{"securities":{}}`

	case "securities":
		log.Println("Security list received.")
		client.handleSecurityList(data)
		break
	}
}

func (client *client) handleSecurityList(data interface{}) {

	numberOfActiveSecurities := 0
	numberOfDeletedSecurities := 0

	exchanges, isOk := data.(map[string]interface{})
	if !isOk {
		log.Printf(`Failed to parse security list: "%s".`, data)
		return
	}

	for exchange, securities := range exchanges {
		security, isOk := securities.(map[string]interface{})
		if !isOk {
			log.Printf(`Failed to parse security: "%s".`, securities)
			return
		}
		for id, securityInterface := range security {
			if securityInterface == nil {
				numberOfDeletedSecurities++
				continue
			}
			securityData, isOk :=
				securityInterface.(map[string]interface{})
			if !isOk {
				log.Printf(`Failed to parse security data: "%s".`, securities)
				return
			}
			symbol, isOk := securityData["name"].(string)
			if !isOk {
				log.Printf(`Failed to parse security name: "%s".`, securities)
				return
			}
			numberOfActiveSecurities++
			if symbol == "BTC/USDT" {
				log.Printf(
					`Requesting "%s" depth of market start...`, symbol)
				client.dataChan <- fmt.Sprintf(`{"dom":{"%s":["%s"]}}`,
					exchange,
					id)
			}
		}
	}

	log.Printf("%d security(ies) is active.", numberOfActiveSecurities)
	if numberOfDeletedSecurities != 0 {
		log.Printf("%d security(ies) removed.", numberOfDeletedSecurities)
	}
}
