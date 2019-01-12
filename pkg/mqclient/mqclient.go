// Copyright 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// DealOrExit creates a new client connection with message broker or exits
// with error printing if creating is failed.
func DealOrExit(broker, nodeType, nodeName string, capacity uint16) *Client {
	result, err := Dial(broker, nodeType, nodeName, capacity)
	if err != nil {
		log.Fatalf(`Failed to connect to MQ broker "%s": "%s".`, broker, err)
	}
	return result
}

// Dial creates a new client connection with message broker.
func Dial(
	broker, nodeType, nodeName string, capacity uint16) (*Client, error) {

	if nodeType == "" {
		return nil, errors.New("Node type is empty")
	}
	if nodeName == "" {
		return nil, errors.New("Node name is empty")
	}

	login := "guest"
	password := "guest"
	urlTemplate := `amqp://%s:%s@` + broker + `:5672/`
	connURL := fmt.Sprintf(urlTemplate, login, password)
	logURL := fmt.Sprintf(urlTemplate, login, "*****")

	var conn *amqp.Connection
	var err error
	for {
		conn, err = amqp.Dial(connURL)
		if err == nil {
			break
		}
		log.Printf(`Failed to connect to the message queuing broker "%s".`, logURL)
		time.Sleep(5 * time.Second)
	}

	result := &Client{
		conn: conn,
		Type: nodeType,
		name: nodeName,
		id:   nodeType + "." + nodeName,
	}
	checkResult := func() (*Client, error) {
		if err != nil {
			result.Close()
			return result, err
		}
		return result, err
	}

	result.directChannel, err = conn.Channel()
	if err != nil {
		return checkResult()
	}
	result.directQueue, err = result.directChannel.QueueDeclare(
		"direct."+result.id,
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		err = fmt.Errorf(`Failed to start unique node: "%s"`, err)
		return checkResult()
	}

	result.Log, err = createLogExchange(conn, capacity)
	if err != nil {
		return checkResult()
	}

	capacityStatus := ""
	if capacity != 1 {
		capacityStatus = fmt.Sprintf(" with capacity %d", capacity)
	}
	result.LogDebugf(`Connected to the message queuing broker "%s"%s.`,
		logURL, capacityStatus)

	return result, nil
}
