// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt

import (
	"log"

	"github.com/Shopify/sarama"
)

// Stream represents a data streaming client.
type Stream struct {
	client sarama.Client
}

func (stream *Stream) init(brokers []string, clientID string) error {

	config := sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Errors = true

	var err error
	stream.client, err = sarama.NewClient(brokers, config)
	if err != nil {
		return err
	}
	return nil
}

func (stream *Stream) close() {
	log.Println("Closing stream client connection...")
	if err := stream.client.Close(); err != nil {
		log.Printf(`Stream client failed to close connection: "%s".`, err)
	}
}
