// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Mq represents a message queuing client.
type Mq struct {
	conn          *amqp.Connection
	directChannel *amqp.Channel
	directQueue   amqp.Queue
}

func (mq *Mq) init(id, broker, login, password string) error {

	var err error
	for {
		mq.conn, err = amqp.Dial(
			fmt.Sprintf("amqp://%s:%s@%s:5672/", login, password, broker))
		if err == nil {
			break
		}
		log.Printf(`Failed to connect to the message queuing broker "%s".`,
			fmt.Sprintf("amqp://%s@%s:5672/", login, broker))
		time.Sleep(5 * time.Second)
	}

	mq.directChannel, err = mq.conn.Channel()
	if err != nil {
		mq.conn.Close()
		return err
	}
	mq.directQueue, err = mq.directChannel.QueueDeclare(
		"direct."+id,
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		err = fmt.Errorf(`Failed to start unique node: "%s"`, err)
		closeChannel(&mq.directChannel)
		mq.conn.Close()
		return err
	}

	return nil
}

func (mq *Mq) close() {
	log.Println("Closing MQ-client connection...")
	closeChannel(&mq.directChannel)
	err := mq.conn.Close()
	if err != nil {
		log.Printf(`MQ-client failed to close connection: "%s".`, err)
	}
}
