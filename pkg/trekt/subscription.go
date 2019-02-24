// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt

import "github.com/Shopify/sarama"

// Subscription represents messages stream from messages sources.
type Subscription interface {
	// Close closes the subscription.
	Close() error

	// todo: return an abstract message type, for MQ and streams.
	Messages() <-chan sarama.ConsumerMessage
}
