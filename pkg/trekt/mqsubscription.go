// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"sync"

	"github.com/streadway/amqp"
)

type mqSubscription struct {
	trekt       Trekt
	mq          MqChannel
	consumerTag string
	queue       amqp.Queue
	messageChan <-chan amqp.Delivery
	stopWaiting sync.WaitGroup
}

func createMqSubscription(
	query string,
	mq MqChannel,
	isAutoAck bool,
	trekt Trekt) (*mqSubscription, error) {

	result := &mqSubscription{}
	err := result.init(query, mq, isAutoAck, trekt)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (subscription *mqSubscription) init(
	query string, mq MqChannel, isAutoAck bool, trekt Trekt) error {

	subscription.trekt = trekt
	subscription.mq = mq

	var err error
	subscription.queue, err = subscription.mq.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil)   // arguments
	if err != nil {
		return nil
	}

	if query == "" {
		query = subscription.queue.Name
	}

	err = subscription.mq.QueueBind(subscription.queue.Name, query, false, nil)
	if err != nil {
		subscription.close()
		return err
	}

	subscription.consumerTag = generateUniqueConsumerTag()
	subscription.messageChan, err = subscription.mq.Consume(
		subscription.queue.Name,  // name
		subscription.consumerTag, // consumer
		isAutoAck,                // auto ack
		false,                    // exclusive
		false,                    // no local
		false,                    // no wait
		nil)                      // args
	if err != nil {
		subscription.close()
		return err
	}

	return nil
}

func (subscription *mqSubscription) close() {
	if subscription.messageChan != nil {
		err := subscription.mq.Cancel(subscription.consumerTag, false)
		if err != nil {
			subscription.trekt.LogErrorf(
				`Failed to cancel consumer subscription "%s": "%s".`,
				subscription.consumerTag, err)
		}
	}

	numberOfTasks, err := subscription.mq.QueueDelete(
		subscription.queue.Name, false, false, false)
	if err != nil {
		subscription.trekt.LogErrorf(
			`Failed to delete subscription queue "%s": "%s".`,
			subscription.queue.Name, err)
	}
	if numberOfTasks > 0 {
		subscription.trekt.LogErrorf(
			`Queue subscription "%s" is canceled`+
				" with %d unhandled items in the queue.",
			subscription.queue.Name, numberOfTasks)
	}
	subscription.stopWaiting.Wait()
}

func (subscription *mqSubscription) handle(handle func(amqp.Delivery)) {
	subscription.stopWaiting.Add(1)
	defer subscription.stopWaiting.Done()
	for {
		message, isOpened := <-subscription.messageChan
		if !isOpened {
			return
		}
		handle(message)
	}
}
