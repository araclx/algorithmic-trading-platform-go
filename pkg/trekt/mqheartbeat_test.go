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

	rpc := mt.NewMockRPCClient(ctrl)
	rpc.EXPECT().Close().
		After(mq.EXPECT().CreateRPCClient().Return(rpc, nil))

	client, err := t.CreateMqHeartbeatClient(mq, trekt)
	if err != nil || client == nil {
		test.Fatalf(`Failed to create client: "%s".`, err)
	}
	defer client.Close()

	request := func(addressIndex int, isError bool) {
		address := fmt.Sprintf("Test address #%d", addressIndex)
		var testErr error
		if isError {
			testErr = fmt.Errorf("Test error for address #%d", addressIndex)
		}

		waiting := sync.WaitGroup{}
		waiting.Add(1)
		rpc.EXPECT().Request(address, true, nil, gomock.Any(), gomock.Any()).Do(
			func(
				address string,
				mandatory bool,
				request interface{},
				handleResponse func([]byte),
				handleError func(error)) {

				go func() {
					defer waiting.Done()
					if testErr != nil {
						handleError(testErr)
					} else {
						handleResponse([]byte{})
					}
				}()
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

// Test_MqHeartbeatClient_Addresses tests heartbeat client work with addresses.
func Test_MqHeartbeatClient_Addresses(test *testing.T) {
	ctrl := gomock.NewController(test)
	defer ctrl.Finish()

	ticksChan := make(chan time.Time)
	defer close(ticksChan)

	ticker := mt.NewMockTicker(ctrl)
	ticker.EXPECT().GetChan().AnyTimes().Return(ticksChan)

	trekt := mt.NewMockTrekt(ctrl)
	trekt.EXPECT().CreateTicker(gomock.Any()).Return(ticker).
		Do(func(...interface{}) { ticker.EXPECT().Stop() })

	mq := mt.NewMockMqChannel(ctrl)

	rpc := mt.NewMockRPCClient(ctrl)
	rpc.EXPECT().Close().
		After(mq.EXPECT().CreateRPCClient().Return(rpc, nil))

	client, err := t.CreateMqHeartbeatClient(mq, trekt)
	if err != nil || client == nil {
		test.Fatalf(`Failed to create client: "%s".`, err)
	}
	defer client.Close()

	waiting := sync.WaitGroup{}
	expect := func(address string) {
		rpc.EXPECT().Request(address, true, nil, gomock.Any(), gomock.Any()).Do(
			func(
				address string,
				mandatory bool,
				request interface{},
				handleResponse func([]byte),
				handleError func(error)) {

				go func() {
					defer waiting.Done()
					handleResponse([]byte{})
				}()
			})
	}
	{
		expect("Test address #1")
		expect("Test address #3")
		expect("Test address #2")
		waiting.Add(3)
		client.AddAddress("Test address #3")
		client.AddAddress("Test address #1")
		client.AddAddress("Test address #2")
		ticksChan <- time.Time{}
		waiting.Wait()
	}
	{
		waiting.Add(2)
		client.RemoveAddress("Test address #2")
		expect("Test address #1")
		expect("Test address #3")
		ticksChan <- time.Time{}
		waiting.Wait()
	}
	{
		waiting.Add(2)
		client.ReplaceAddress(map[string]interface{}{
			"Test address #4": nil, "Test address #5": nil})
		expect("Test address #5")
		expect("Test address #4")
		ticksChan <- time.Time{}
		waiting.Wait()
	}

}
