// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt

// StreamChannel represents a message stream channel of TREKT-network.
type StreamChannel interface {
	// Close closes the channel.
	Close()

	// CreateSubscription creates a subscription at messages by required topic.
	CreateSubscription(topic string, offset int64) (Subscription, error)
}

type streamChannel struct {
	trekt  Trekt
	stream *Stream
}

func (channel *streamChannel) init(
	stream *Stream, trekt Trekt) error {
	channel.trekt = trekt
	channel.stream = stream
	return nil
}

func (*streamChannel) Close() {}

func (channel *streamChannel) CreateSubscription(
	topic string, offset int64) (Subscription, error) {

	return createStreamSubscription(topic, offset, channel)
}
