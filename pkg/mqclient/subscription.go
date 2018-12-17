// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"fmt"

	"github.com/streadway/amqp"
)

type messageSubscription struct {
	exchange    *exchange
	consumerTag string
	reportError func(string)
	queue       amqp.Queue
	messageChan <-chan amqp.Delivery
}

func (subscription *messageSubscription) init(
	request string,
	exchange *exchange,
	isAutoAck bool,
	reportError func(string)) error {

	subscription.exchange = exchange
	subscription.reportError = reportError

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
	if request == "" {
		request = subscription.queue.Name
	}

	err = subscription.exchange.channel.QueueBind(
		subscription.queue.Name, request, exchange.name, false, nil)
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

func (subscription *messageSubscription) close() {
	if subscription.messageChan != nil {
		err := subscription.exchange.channel.Cancel(subscription.consumerTag, false)
		if err != nil {
			subscription.reportError(fmt.Sprintf(
				`Failed to cancel consumer subscription "%s": "%s"`,
				subscription.consumerTag, err))
		}
	}

	numberOfTasks, err := subscription.exchange.channel.QueueDelete(
		subscription.queue.Name, false, false, false)
	if err != nil {
		subscription.reportError(
			fmt.Sprintf(`Failed to delete subscription queue "%s": "%s"`,
				subscription.queue.Name, err))
	}
	if numberOfTasks > 0 {
		subscription.reportError(fmt.Sprintf(
			`Queue subscription "%s" is canceled with %d unhandled items in the queue.`,
			subscription.queue.Name, numberOfTasks))
	}
}

func (subscription *messageSubscription) handle(handle func(amqp.Delivery)) {
	for {
		message, isOpened := <-subscription.messageChan
		if !isOpened {
			break
		}
		handle(message)
	}
}
