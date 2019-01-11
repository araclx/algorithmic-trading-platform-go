// Copyright 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"fmt"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

type subscription struct {
	exchange    *exchange
	consumerTag string
	reportError func(string)
	queue       amqp.Queue
	messageChan <-chan amqp.Delivery
}

func createSubscription(
	query string,
	exchange *exchange,
	isAutoAck bool,
	reportError func(string)) (*subscription, error) {

	result := &subscription{}
	err := result.init(query, exchange, isAutoAck, reportError)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (subscription *subscription) init(
	query string,
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

func (subscription *subscription) close() {
	if subscription.messageChan != nil {
		err := subscription.exchange.channel.Cancel(subscription.consumerTag, false)
		if err != nil {
			subscription.reportError(fmt.Sprintf(
				`Failed to cancel consumer subscription "%s": "%s"`,
				subscription.consumerTag, err))
		}
		subscription.messageChan = nil
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

func (subscription *subscription) handle(handle func(amqp.Delivery)) {
	for {
		message, isOpened := <-subscription.messageChan
		if !isOpened {
			break
		}
		handle(message)
	}
}

///////////////////////////////////////////////////////////////////////////////

type clientSubscription struct {
	subscription
	client *Client
}

func createClientSubscription(
	query string,
	exchange *exchange,
	isAutoAck bool,
	client *Client) (*clientSubscription, error) {

	result := &clientSubscription{}
	err := result.init(query, exchange, isAutoAck, client)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (subscription *clientSubscription) init(query string,
	exchange *exchange,
	isAutoAck bool,
	client *Client) error {

	subscription.client = client
	return subscription.subscription.init(
		query, exchange, isAutoAck,
		func(message string) { subscription.client.LogError(message + ".") })
}

///////////////////////////////////////////////////////////////////////////////
