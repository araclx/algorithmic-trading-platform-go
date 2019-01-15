// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"github.com/streadway/amqp"
)

type mqSubscription struct {
	exchange    *mqExchange
	consumerTag string
	queue       amqp.Queue
	messageChan <-chan amqp.Delivery
}

func createMqSubscription(
	query string,
	exchange *mqExchange,
	isAutoAck bool) (*mqSubscription, error) {

	result := &mqSubscription{}
	err := result.init(query, exchange, isAutoAck)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (subscription *mqSubscription) init(
	query string,
	exchange *mqExchange,
	isAutoAck bool) error {

	subscription.exchange = exchange

	var err error
	subscription.queue, err = exchange.channel.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil
	}

	if query == "" {
		query = subscription.queue.Name
	}

	err = subscription.exchange.channel.QueueBind(
		subscription.queue.Name, query, exchange.name, false, nil)
	if err != nil {
		subscription.close()
		return err
	}

	subscription.consumerTag = generateUniqueConsumerTag()
	subscription.messageChan, err = subscription.exchange.channel.Consume(
		subscription.queue.Name,  // name
		subscription.consumerTag, // consumer
		isAutoAck,                // auto ack
		false,                    // exclusive
		false,                    // no local
		false,                    // no wait
		nil,                      // args
	)
	if err != nil {
		subscription.close()
		return err
	}

	return nil
}

func (subscription *mqSubscription) close() {
	if subscription.messageChan != nil {
		err := subscription.exchange.channel.Cancel(subscription.consumerTag, false)
		if err != nil {
			subscription.exchange.trekt.LogErrorf(
				`Failed to cancel consumer subscription "%s": "%s".`,
				subscription.consumerTag, err)
		}
		subscription.messageChan = nil
	}

	numberOfTasks, err := subscription.exchange.channel.QueueDelete(
		subscription.queue.Name, false, false, false)
	if err != nil {
		subscription.exchange.trekt.LogErrorf(
			`Failed to delete subscription queue "%s": "%s".`,
			subscription.queue.Name, err)
	}
	if numberOfTasks > 0 {
		subscription.exchange.trekt.LogErrorf(
			`Queue subscription "%s" is canceled with %d unhandled items in the queue.`,
			subscription.queue.Name, numberOfTasks)
	}
}

func (subscription *mqSubscription) handle(handle func(amqp.Delivery)) {
	for {
		message, isOpened := <-subscription.messageChan
		if !isOpened {
			break
		}
		handle(message)
	}
}
