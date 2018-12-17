// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

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
		dataChan: make(chan string, 10000),
	}
}

func (client *client) close() {
	close(client.dataChan)
	client.conn.Close()
}

func (client *client) run() {
	client.dataChan <- `[
		{
			"topic": "auth",
			"data": {
				"login": "guest",
				"password": "guest"
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
			var messages []map[string]interface{}
			err = json.Unmarshal(data, &messages)
			if err != nil {
				log.Printf(
					`Failed to parse data from the connection: "%s".`, err)
				return
			}
			go func() {
				for _, message := range messages {
					client.handleServerMessage(message)
				}
			}()
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

func (client *client) handleServerMessage(message map[string]interface{}) {}
