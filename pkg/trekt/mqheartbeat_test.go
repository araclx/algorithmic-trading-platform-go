// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mt "github.com/rektra-network/trekt-go/mock/pkg/trekt"
	t "github.com/rektra-network/trekt-go/pkg/trekt"
	"github.com/streadway/amqp"
)

// Test_MqHeartbeatServer tests heartbeat server work.
func Test_MqHeartbeatServer(test *testing.T) {
	ctrl := gomock.NewController(test)
	defer ctrl.Finish()

	trekt := mt.NewMockTrekt(ctrl)

	mq := mt.NewMockMqChannel(ctrl)
	mqQueueName := "test queue"
	mq.EXPECT().QueueDelete(mqQueueName, false, false, false).Return(0, nil).
		After(mq.EXPECT().QueueDeclare("", false, true, true, false, nil).
			Return(amqp.Queue{Name: mqQueueName}, nil))
	mq.EXPECT().QueueBind(mqQueueName, mqQueueName, false, nil).Return(nil)
	requestsChan := make(chan amqp.Delivery)
	mq.EXPECT().Consume(
		mqQueueName, gomock.Any(), true, false, false, false, nil).
		Do(
			func(string, consumer string, autoAck, exclusive, noLocal, noWait, args interface{}) {
				mq.EXPECT().Cancel(consumer, false).
					Do(func(...interface{}) { close(requestsChan) }).
					Return(nil)
			}).
		Return(requestsChan, nil)

	server, err := t.CreateMqHeartbeatServer(mq, trekt)
	if err != nil || server == nil {
		test.Fatalf(`Failed to create server: "%s".`, err)
	}
	defer server.Close()

	send := func(correlation, replyTo int) {
		delivery := amqp.Delivery{
			CorrelationId: fmt.Sprintf("Correlation ID #%d", correlation),
			ReplyTo:       fmt.Sprintf("reply address #%d", replyTo)}
		publishing := amqp.Publishing{
			CorrelationId: delivery.CorrelationId,
			ContentType:   "application/json"}
		publishing.Body, _ = json.Marshal(nil)
		mq.EXPECT().Publish(delivery.ReplyTo, false, false, publishing)
		delivery.Body = publishing.Body
		requestsChan <- delivery
	}

	send(1, 2)
	send(2, 2)
	send(3, 1)
	send(4, 2)
	send(5, 3)
}

// Test_MqHeartbeatClient_Tests tests heartbeat client general work.
func Test_MqHeartbeatClient_Tests(test *testing.T) {
	ctrl := gomock.NewController(test)
	defer ctrl.Finish()

	ticksChan := make(chan time.Time)
	defer close(ticksChan)

	ticker := mt.NewMockTicker(ctrl)
	ticker.EXPECT().GetChan().AnyTimes().Return(ticksChan)

	trekt := mt.NewMockTrekt(ctrl)
	trekt.EXPECT().CreateTicker(1 * time.Minute).Return(ticker).
		Do(func(...interface{}) { ticker.EXPECT().Stop() })

	mq := mt.NewMockMqChannel(ctrl)
	mqQueueName := "test client queue"
	mq.EXPECT().QueueDelete(mqQueueName, false, false, false).Return(0, nil).
		After(mq.EXPECT().QueueDeclare("", false, true, true, false, nil).
			Return(amqp.Queue{Name: mqQueueName}, nil))
	mq.EXPECT().QueueBind(mqQueueName, mqQueueName, false, nil).Return(nil)
	requestsChan := make(chan amqp.Delivery)
	mq.EXPECT().Consume(
		mqQueueName, gomock.Any(), true, false, false, false, nil).
		Do(
			func(string, consumer string, autoAck,
				exclusive, noLocal, noWait, args interface{}) {

				mq.EXPECT().Cancel(consumer, false).
					Do(func(...interface{}) { close(requestsChan) }).
					Return(nil)
			}).
		Return(requestsChan, nil)

	client, err := t.CreateMqHeartbeatClient(mq, trekt)
	if err != nil || client == nil {
		test.Fatalf(`Failed to create client: "%s".`, err)
	}
	defer client.Close()

	request := func(addressIndex int, isError bool) {
		address := fmt.Sprintf("Test address #%d", addressIndex)
		publishing := amqp.Publishing{
			CorrelationId: "1234",
			ReplyTo:       mqQueueName,
			ContentType:   "application/json"}
		var testErr error
		if isError {
			testErr = fmt.Errorf("Test error for address #%d", addressIndex)
		}

		waiting := sync.WaitGroup{}
		waiting.Add(1)
		mq.EXPECT().RegisterHandlers(publishing, gomock.Any(), gomock.Any()).
			Do(func(
				request amqp.Publishing,
				handleResponse func(amqp.Delivery),
				handleError func(error)) {

				mq.EXPECT().Publish(address, true, false, publishing).
					Do(func(...interface{}) {
						defer waiting.Done()
						if testErr != nil {
							go handleError(testErr)
						} else {
							go handleResponse(amqp.Delivery{ContentType: "application/json"})
						}
					})
			})
		client.AddAddress(address)
		defer client.RemoveAddress(address)
		ticksChan <- time.Time{}

		if testErr == nil {
			waiting.Wait()
			return
		}

		fail := <-client.GetFailedTestsChan()
		if fail.Address != address {
			test.Errorf(`Wrong address: "%v".`, fail)
		}
		if fail.Err != testErr {
			test.Errorf(`Wrong error: "%v".`, fail)
		}
	}

	request(1, false)
	request(1, true)
	request(2, false)
	request(3, true)

}
