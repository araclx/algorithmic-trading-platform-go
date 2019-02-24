// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt

import (
	"context"

	"github.com/Shopify/sarama"
)

type streamSubscription struct {
	channel      *streamChannel
	consumer     sarama.ConsumerGroup
	messagesChan chan sarama.ConsumerMessage
}

func createStreamSubscription(
	topic string, offset int64, channel *streamChannel) (
	*streamSubscription, error) {

	result := &streamSubscription{}
	if err := result.init(topic, channel); err != nil {
		return nil, err
	}
	return result, nil
}

func (subscription *streamSubscription) init(
	topic string, channel *streamChannel) error {

	subscription.channel = channel

	var err error
	subscription.consumer, err = sarama.NewConsumerGroupFromClient(
		topic, subscription.channel.stream.client)
	if err != nil {
		return err
	}

	subscription.messagesChan = make(chan sarama.ConsumerMessage, 1)
	context := context.Background()
	go func() {
		defer close(subscription.messagesChan)
		for {
			err := subscription.consumer.Consume(context, []string{topic},
				&logRecordHandler{
					trekt:        subscription.channel.trekt,
					messagesChan: subscription.messagesChan})
			if err != nil {
				if err != sarama.ErrClosedConsumerGroup {
					subscription.channel.trekt.LogErrorf(
						`Failed to consume stream-message for "%s": "%s".`, topic, err)
				}
				break
			}
		}
	}()

	return nil
}

func (subscription *streamSubscription) Close() error {
	return subscription.consumer.Close()
}

func (subscription *streamSubscription) Messages() <-chan sarama.ConsumerMessage {
	return subscription.messagesChan
}
