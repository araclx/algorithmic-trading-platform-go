// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	mt "github.com/rektra-network/trekt-go/mock/pkg/trekt"
	tl "github.com/rektra-network/trekt-go/pkg/tradinglib"
	t "github.com/rektra-network/trekt-go/pkg/trekt"
	"github.com/streadway/amqp"
)

////////////////////////////////////////////////////////////////////////////////

type marketDataServiceContext struct {
	test         *testing.T
	ctrl         *gomock.Controller
	testsWaiting sync.WaitGroup
	trekt        *mt.MockTrekt
	exchange     *mt.MockMarketDataExchange
}

func createMarketDataServiceContext(test *testing.T) *marketDataServiceContext {

	ctrl := gomock.NewController(test)
	result := &marketDataServiceContext{
		test:     test,
		ctrl:     ctrl,
		trekt:    mt.NewMockTrekt(ctrl),
		exchange: mt.NewMockMarketDataExchange(ctrl)}

	result.trekt.EXPECT().LogDebugf(gomock.Any()).AnyTimes()
	result.trekt.EXPECT().LogDebugf(gomock.Any(), gomock.Any()).AnyTimes()
	result.trekt.EXPECT().LogDebugf(
		gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()
	result.trekt.EXPECT().LogDebugf(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()

	result.exchange.EXPECT().GetTrekt().AnyTimes().Return(result.trekt)
	return result
}

func (ctx *marketDataServiceContext) close() {
	ctx.testsWaiting.Wait()
	ctx.ctrl.Finish()
}

func (ctx *marketDataServiceContext) createSubscriptionMqRpcMock(
	channel *mt.MockMqChannel) *mt.MockRPCClient {

	result := mt.NewMockRPCClient(ctx.ctrl)
	result.EXPECT().Close().Do(func(...interface{}) { ctx.testsWaiting.Done() }).
		After(
			channel.EXPECT().CreateRPCClient().Return(result, nil).Do(
				func(...interface{}) {
					ctx.testsWaiting.Add(1)
				}))
	return result
}

func (ctx *marketDataServiceContext) expectMqRpcRequest(
	rpc *mt.MockRPCClient,
	heartbeat *mt.MockMqHeartbeatServer,
	subscriber string,
	security tl.Security,
	getStartRequestSuccess func(interface{}) *struct {
		response []byte
		offset   int64
	},
	getStartRequestFail func() error,
	getStopRequestSuccess func(interface{}) *[]byte,
	getStopRequestFail func() error,
	execWaiting *sync.WaitGroup) *sync.WaitGroup {

	if execWaiting == nil {
		execWaiting = &sync.WaitGroup{}
	}
	execWaiting.Add(1)

	heartbeat.EXPECT().GetAddress().Times(2).Return(subscriber)

	rpc.EXPECT().Request(
		security.Exchange, // routing key
		true,              // mandatory
		t.MarketDataRequest{ // request
			Subscriber: subscriber,
			Security:   security.ID,
			IsStart:    false},
		gomock.Any(),  // success handler
		gomock.Any()). // fail handler
		Do(func(routingKey string,
			mandatory bool,
			request interface{},
			handleSuccess func([]byte),
			handleFail func(error)) {

			defer execWaiting.Done()
			if success := getStopRequestSuccess(request); success != nil {
				ctx.trekt.EXPECT().LogInfof(
					`Market data for "%s" on "%s" is stopped.`,
					security.ID, security.Exchange)
				handleSuccess(*success)
			}
			if err := getStopRequestFail(); err != nil {
				ctx.trekt.EXPECT().LogErrorf(
					`Failed to stop market data for "%s" on "%s": "%s".`,
					security.ID, security.Exchange, err)
				handleFail(err)
			}
		}).
		After(rpc.EXPECT().Request(
			security.Exchange, // routing key
			true,              // mandatory
			t.MarketDataRequest{ // request
				Subscriber: subscriber,
				Security:   security.ID,
				IsStart:    true},
			gomock.Any(),  // success handler
			gomock.Any()). // fail handler
			Do(func(routingKey string,
				mandatory bool,
				request interface{},
				handleSuccess func([]byte),
				handleFail func(error)) {

				if response := getStartRequestSuccess(request); response != nil {
					ctx.trekt.EXPECT().LogInfof(
						`Market data for "%s" on "%s" is started from update %d.`,
						security.ID, security.Exchange, response.offset)
					handleSuccess(response.response)
				}
				if err := getStartRequestFail(); err != nil {
					ctx.trekt.EXPECT().LogErrorf(
						`Failed to start market data for "%s" on "%s": "%s".`,
						security.ID, security.Exchange, err)
					handleFail(err)
				}
			}))

	return execWaiting
}

func (ctx *marketDataServiceContext) createMqChannelMock() (
	*mt.MockMqChannel, *mt.MockMqHeartbeatServer) {

	result := mt.NewMockMqChannel(ctx.ctrl)
	heartbeat := mt.NewMockMqHeartbeatServer(ctx.ctrl)
	result.EXPECT().CreateHeartbeatServer().AnyTimes().
		Do(func(...interface{}) { heartbeat.EXPECT().Close() }).
		Return(heartbeat, nil)
	return result, heartbeat
}

func (ctx *marketDataServiceContext) createStreamSubscriptionMock(
	channel *mt.MockStreamChannel,
	topic string,
	offset int64) *mt.MockSubscription {

	result := mt.NewMockSubscription(ctx.ctrl)
	result.EXPECT().Close().Return(nil).Do(func(...interface{}) {
		ctx.testsWaiting.Done()
	}).After(
		channel.EXPECT().CreateSubscription(topic, offset).Return(result, nil).Do(
			func(...interface{}) { ctx.testsWaiting.Add(1) }))
	return result
}

func (ctx *marketDataServiceContext) areDomUpdatesEqual(
	security tl.Security,
	update t.DepthOfMarketUpdate,
	controlUpdate map[float64]t.DepthOfMarketLevel) bool {

	if update.Security != security || len(controlUpdate) != len(update.Levels) {
		return false
	}
	for _, level := range update.Levels {
		controlLevel, has := controlUpdate[level.GetKey()]
		if !has {
			return false
		}
		if level.GetKey() != controlLevel.GetKey() ||
			level.GetPrice() != controlLevel.GetPrice() ||
			level.GetQty() != controlLevel.GetQty() ||
			level.IsBid() != controlLevel.IsBid() {
			return false
		}
	}
	return true
}

func (ctx *marketDataServiceContext) testMessagesSnapshot(
	security tl.Security,
	snapshotsChan <-chan t.DepthOfMarketUpdate,
	snapshot map[float64]t.DepthOfMarketLevel) {

	message := <-snapshotsChan
	if !ctx.areDomUpdatesEqual(security, message, snapshot) {
		ctx.test.Errorf("Snapshot is wrong: %v != %v.", message, snapshot)
	}
}

func (ctx *marketDataServiceContext) testMessagesChan(
	security tl.Security,
	inChan chan<- sarama.ConsumerMessage,
	outChans []<-chan t.DepthOfMarketUpdate,
	waitForReceiving bool,
	seed float64) map[float64]t.DepthOfMarketLevel {

	clone := func(source map[float64]t.DepthOfMarketLevel) map[float64]t.DepthOfMarketLevel {
		result := map[float64]t.DepthOfMarketLevel{}
		for key, level := range source {
			result[key] = level.Clone()
		}
		return result
	}

	snapshots := []map[float64]t.DepthOfMarketLevel{{
		-10 - seed: t.DepthOfMarketLevel{-10 - seed, 11},
		-8 - seed:  t.DepthOfMarketLevel{-8 - seed, 12},
		-6 - seed:  t.DepthOfMarketLevel{-6 - seed, 13},
		-4 - seed:  t.DepthOfMarketLevel{-4 - seed, 14},
		-2 - seed:  t.DepthOfMarketLevel{-2 - seed, 15},
		2 + seed:   t.DepthOfMarketLevel{2 + seed, 16},
		4 + seed:   t.DepthOfMarketLevel{4 + seed, 17},
		6 + seed:   t.DepthOfMarketLevel{6 + seed, 18},
		8 + seed:   t.DepthOfMarketLevel{8 + seed, 19},
		10 + seed:  t.DepthOfMarketLevel{10 + seed, 20}}}

	updates := []map[float64]t.DepthOfMarketLevel{
		{
			-10 - seed: t.DepthOfMarketLevel{-10 - seed, 0},
			-6 - seed:  t.DepthOfMarketLevel{-6 - seed, 0},
			-7 - seed:  t.DepthOfMarketLevel{-7 - seed, 23},
			-1 - seed:  t.DepthOfMarketLevel{-1 - seed, 25}},
		{
			2 + seed:  t.DepthOfMarketLevel{2 + seed, 0},
			4 + seed:  t.DepthOfMarketLevel{4 + seed, 0},
			5 + seed:  t.DepthOfMarketLevel{5 + seed, 37},
			11 + seed: t.DepthOfMarketLevel{11 + seed, 32}},
		{
			-8 - seed: t.DepthOfMarketLevel{-8 - seed, 42},
			8 + seed:  t.DepthOfMarketLevel{8 + seed, 49}},
		{
			-12 - seed: t.DepthOfMarketLevel{-12 - seed, 555},
			12 + seed:  t.DepthOfMarketLevel{12 + seed, 555}}}

	lastSnapshot := clone(snapshots[0])
	for _, update := range updates {
		for key, level := range update {
			if !level.IsDeleted() {
				lastSnapshot[key] = level
			} else {
				delete(lastSnapshot, key)
			}
		}
		snapshots = append(snapshots, clone(lastSnapshot))
	}
	{
		removes := map[float64]t.DepthOfMarketLevel{}
		for key, level := range lastSnapshot {
			level.SetDeleted()
			removes[key] = level
		}
		updates = append(updates, removes)
		snapshots = append(snapshots, map[float64]t.DepthOfMarketLevel{})
	}
	lastSnapshot = clone(snapshots[0])
	for _, level := range lastSnapshot {
		level.Set(level.GetPrice(), level.GetQty()+1000, level.IsBid())
	}
	snapshots = append(snapshots, clone(lastSnapshot))
	updates = append(updates, clone(lastSnapshot))

	waiting := sync.WaitGroup{}

	waiting.Add(1)
	go func() {
		defer waiting.Done()
		export := func(
			source map[float64]t.DepthOfMarketLevel) t.DepthOfMarketLevels {

			result := make(t.DepthOfMarketLevels, len(source))
			i := 0
			for _, level := range source {
				result[i] = level
				i++
			}
			return result
		}
		{
			message := sarama.ConsumerMessage{
				Key:   []byte(fmt.Sprintf("Key snapshot")),
				Topic: fmt.Sprintf("Topic snapshot")}
			var err error
			message.Value, err = json.Marshal(export(snapshots[0]))
			if err != nil {
				ctx.test.Fatal(err)
				return
			}
			inChan <- message
		}
		for _, update := range updates {
			value, err := json.Marshal(export(update))
			if err != nil {
				ctx.test.Fatal(err)
			}
			inChan <- sarama.ConsumerMessage{Value: []byte(value)}
		}
	}()

	if !waitForReceiving {
		ctx.testsWaiting.Add(len(outChans))
	} else {
		waiting.Add(len(outChans))
	}
	for _, outChan := range outChans {
		go func(
			security tl.Security,
			outChan <-chan t.DepthOfMarketUpdate) {

			if !waitForReceiving {
				defer ctx.testsWaiting.Done()
			} else {
				defer waiting.Done()
			}

			i := -1
			for {
				update, isOpen := <-outChan
				if !isOpen {
					return
				}

				if i < 0 {
					for snapshotI, snapshot := range snapshots {
						if ctx.areDomUpdatesEqual(security, update, snapshot) {
							i = snapshotI
							break
						}
					}
					if i < 0 {
						ctx.test.Errorf(`Failed to find snapshot by first update "%v".`,
							update)
						return
					}
					continue
				}
				if len(updates) <= i {
					ctx.test.Errorf("Too many incremental messages received: %d.", i)
					return
				}

				controlUpdate := updates[i]
				if !ctx.areDomUpdatesEqual(security, update, controlUpdate) {
					ctx.test.Errorf("Incremental message %d is wrong:  %v != %v.",
						i, update, controlUpdate)
					return
				}

				i++

				if waitForReceiving && i >= len(updates) {
					return
				}
			}
		}(security, outChan)
	}

	waiting.Wait()

	return lastSnapshot
}

// Test_MarketData_Service_ErrorCreation tests CreateMarketDataService error.
func Test_MarketData_Service_ErrorCreation(test *testing.T) {
	ctx := createMarketDataServiceContext(test)
	defer ctx.close()

	commandsChannel, _ := ctx.createMqChannelMock()
	testErr := errors.New("Test error")
	commandsChannel.EXPECT().CreateRPCClient().Return(nil, testErr)

	dataChannel := mt.NewMockStreamChannel(ctx.ctrl)

	service, err := t.CreateMarketDataService(
		ctx.exchange, commandsChannel, dataChannel, 1)

	if err == nil {
		test.Error("Error expected.")
	} else if err != testErr {
		test.Errorf(`Wrong error: "%s".`, err)
	}
	if service != nil {
		test.Errorf("Nil instead service expected.")
	}

}

// Test_MarketData_Service_Dom_ErrorSubscription tests depth of market
// subscription creation error.
func Test_MarketData_Service_Dom_ErrorSubscription(
	test *testing.T) {

	ctx := createMarketDataServiceContext(test)
	defer ctx.close()

	commandsChannel, heartbeat := ctx.createMqChannelMock()
	dataChannel := mt.NewMockStreamChannel(ctx.ctrl)

	security := tl.Security{
		Exchange: "Test exchange with error",
		ID:       "Test security with error"}

	rpc := ctx.createSubscriptionMqRpcMock(commandsChannel)

	service, err := t.CreateMarketDataService(
		ctx.exchange, commandsChannel, dataChannel, 1)
	if service == nil {
		test.Fatalf("Failed to create service.")
	}
	defer service.Close()
	if err != nil {
		test.Fatalf(`Failed to create service: "%s".`, err)
	}

	subscriptionWaiting := sync.WaitGroup{}
	rpcExecWaiting := ctx.expectMqRpcRequest(
		rpc,
		heartbeat,
		"Subscriber",
		security,
		func(interface{}) *struct {
			response []byte
			offset   int64
		} {
			result := &struct {
				response []byte
				offset   int64
			}{offset: 99224411}
			testErr := errors.New("Test error")
			ctx.trekt.EXPECT().LogErrorf(
				`Failed to start market data stream subscription "%s" on "%s": "%s".`,
				security.ID, security.Exchange, testErr).
				Do(func(...interface{}) { subscriptionWaiting.Done() }).
				After(dataChannel.EXPECT().CreateSubscription("md", result.offset).
					Return(nil, testErr))
			result.response, _ = json.Marshal(
				&t.MarketDataStartRequestResponse{Start: result.offset})
			return result
		},
		func() error { return nil },
		func(interface{}) *[]byte { return &[]byte{} },
		func() error { return nil },
		nil)

	subscriptionWaiting.Add(1)
	updatesChan := make(chan t.DepthOfMarketUpdate)
	defer close(updatesChan)
	var subscription *t.MarketDataSubscription
	subscription, err = service.StartDepthOfMarket(security, updatesChan)
	if subscription == nil {
		test.Fatal("DOM subscription failed, bun nil-result is not expected.")
		return
	}
	if err != nil {
		test.Error("DOM subscription has failed, but error is not expected.")
	}

	subscriptionWaiting.Wait()
	subscription.Close()
	rpcExecWaiting.Wait()

}

// Test_MarketData_Service_Dom_ErrorRequest tests depth of market data start and
// stop requests error.
func Test_MarketData_Service_Dom_ErrorRequest(test *testing.T) {
	ctx := createMarketDataServiceContext(test)
	defer ctx.close()

	commandsChannel, heartbeat := ctx.createMqChannelMock()
	dataChannel := mt.NewMockStreamChannel(ctx.ctrl)

	security := tl.Security{
		Exchange: "Test exchange with error",
		ID:       "Test security with error"}

	rpc := ctx.createSubscriptionMqRpcMock(commandsChannel)
	service, err := t.CreateMarketDataService(
		ctx.exchange, commandsChannel, dataChannel, 1)
	if err != nil || service == nil {
		test.Fatalf(`Failed to create service: "%s".`, err)
	}
	defer service.Close()

	rpcExecWaiting := ctx.expectMqRpcRequest(
		rpc,
		heartbeat,
		"Subscriber",
		security,
		func(interface{}) *struct {
			response []byte
			offset   int64
		} {
			return nil
		},
		func() error { return errors.New("Test start error") },
		func(interface{}) *[]byte { return nil },
		func() error { return errors.New("Test stop error") },
		nil)

	{
		var dom *t.MarketDataSubscription
		dom, err = service.StartDepthOfMarket(security, nil)
		if err != nil || dom == nil {
			test.Fatalf(`Failed to create DOM: "%s".`, err)
		}
		dom.Close()
	}

	rpcExecWaiting.Wait()

}

// Test_MarketData_Service_Dom_Snapshot tests initial snapshot.
func Test_MarketData_Service_Dom_Snapshot(test *testing.T) {
	ctx := createMarketDataServiceContext(test)
	defer ctx.close()

	commandsChannel, heartbeat := ctx.createMqChannelMock()
	dataChannel := mt.NewMockStreamChannel(ctx.ctrl)

	security := tl.Security{
		Exchange: "Test exchange with error",
		ID:       "Test security with error"}

	rpc := ctx.createSubscriptionMqRpcMock(commandsChannel)
	service, err := t.CreateMarketDataService(
		ctx.exchange, commandsChannel, dataChannel, 1)
	if err != nil || service == nil {
		test.Fatalf(`Failed to create service: "%s".`, err)
	}
	defer service.Close()

	messageChan := make(chan sarama.ConsumerMessage, 100)
	defer close(messageChan)

	snapshot := map[float64]t.DepthOfMarketLevel{
		-1010: t.DepthOfMarketLevel{-1010, 1011},
		-108:  t.DepthOfMarketLevel{-108, 1012},
		-106:  t.DepthOfMarketLevel{-106, 1013},
		-104:  t.DepthOfMarketLevel{-104, 1014},
		-102:  t.DepthOfMarketLevel{-102, 1015},
		102:   t.DepthOfMarketLevel{102, 1016},
		104:   t.DepthOfMarketLevel{104, 1017},
		106:   t.DepthOfMarketLevel{106, 1018},
		108:   t.DepthOfMarketLevel{108, 1019},
		1010:  t.DepthOfMarketLevel{1010, 1020}}

	rpcExecWaiting := ctx.expectMqRpcRequest(
		rpc,
		heartbeat,
		"Subscriber",
		security,
		func(interface{}) *struct {
			response []byte
			offset   int64
		} {
			result := &struct {
				response []byte
				offset   int64
			}{offset: 992244321}
			ctx.createStreamSubscriptionMock(dataChannel, "md", result.offset).
				EXPECT().Messages().AnyTimes().Return(messageChan)
			response := t.MarketDataStartRequestResponse{
				Snapshot: t.DepthOfMarketLevels{},
				Start:    result.offset}
			for _, level := range snapshot {
				response.Snapshot = append(response.Snapshot, level)
			}
			result.response, _ = json.Marshal(&response)
			return result
		},
		func() error { return nil },
		func(interface{}) *[]byte { return &[]byte{} },
		func() error { return nil },
		nil)

	updatesChan := make(chan t.DepthOfMarketUpdate, 1)
	defer close(updatesChan)
	subscription, err := service.StartDepthOfMarket(security, updatesChan)
	if err != nil || subscription == nil {
		test.Fatalf(`Failed to create DOM: "%s".`, err)
	}
	ctx.testMessagesSnapshot(security, updatesChan, snapshot)

	subscription.Close()
	rpcExecWaiting.Wait()

}

// Test_MarketData_Service_Dom_Workflow tests general depth of market
// subscription workflow.
func Test_MarketData_Service_Dom_Workflow(test *testing.T) {
	numberOfClients := 10
	numberOfClientSubsctrbitions := 10

	ctx := createMarketDataServiceContext(test)
	defer ctx.close()

	commandsChannel, heartbeat := ctx.createMqChannelMock()
	dataChannel := mt.NewMockStreamChannel(ctx.ctrl)

	security := tl.Security{
		Exchange: "Test exchange with error",
		ID:       "Test security with error"}

	rpc := ctx.createSubscriptionMqRpcMock(commandsChannel)
	service, err := t.CreateMarketDataService(
		ctx.exchange, commandsChannel, dataChannel, 1)
	if err != nil || service == nil {
		test.Fatalf(`Failed to create service: "%s".`, err)
	}
	defer service.Close()

	messageChan := make(chan sarama.ConsumerMessage, 100)
	defer close(messageChan)

	rpcExecWaiting := ctx.expectMqRpcRequest(
		rpc,
		heartbeat,
		"Subscriber",
		security,
		func(interface{}) *struct {
			response []byte
			offset   int64
		} {
			result := &struct {
				response []byte
				offset   int64
			}{offset: 992244321}
			ctx.createStreamSubscriptionMock(dataChannel, "md", result.offset).
				EXPECT().Messages().AnyTimes().Return(messageChan)
			result.response, _ = json.Marshal(
				t.MarketDataStartRequestResponse{Start: result.offset})
			return result
		},
		func() error { return nil },
		func(interface{}) *[]byte { return &[]byte{} },
		func() error { return nil },
		nil)

	// Initial requests:
	subscribers := make([]*t.MarketDataSubscription,
		numberOfClients*numberOfClientSubsctrbitions)
	updatesChans := make([]<-chan t.DepthOfMarketUpdate, numberOfClients)
	for client := range updatesChans {
		updatesChan := make(chan t.DepthOfMarketUpdate, 100)
		defer close(updatesChan)
		updatesChans[client] = updatesChan
		for subscriber := 0; subscriber < numberOfClientSubsctrbitions; subscriber++ {
			subscription, err := service.StartDepthOfMarket(security, updatesChan)
			if err != nil || subscription == nil {
				test.Fatalf(`Failed to create DOM: "%s".`, err)
			}
			subscribers[(client*numberOfClientSubsctrbitions)+subscriber] =
				subscription
		}
	}
	snapshot :=
		ctx.testMessagesChan(security, messageChan, updatesChans, true, 0)

	// New requests when service already works with security:
	for client := 0; client < numberOfClients; client++ {
		updatesChan := make(chan t.DepthOfMarketUpdate, 100)
		defer close(updatesChan)
		updatesChans = append(updatesChans, updatesChan)
		for subscriber := 0; subscriber < numberOfClientSubsctrbitions; subscriber++ {
			subscription, err := service.StartDepthOfMarket(security, updatesChan)
			if err != nil || subscription == nil {
				test.Fatalf(`Failed to create DOM: "%s".`, err)
			}
			subscribers = append(subscribers, subscription)
		}
		ctx.testMessagesSnapshot(security, updatesChan, snapshot)
	}

	// Server sent "start book from the scratch".
	if len(snapshot) == 0 {
		ctx.test.Error(
			`Could not test "book from the scratch" as last snapshot is empty.`)
	}
	for _, level := range snapshot {
		level.SetDeleted()
	}
	go func() {
		message := sarama.ConsumerMessage{
			Key:   []byte(fmt.Sprintf("Key snapshot")),
			Topic: fmt.Sprintf("Topic snapshot")}
		var err error
		message.Value, err = json.Marshal(&t.DepthOfMarketLevels{})
		if err != nil {
			ctx.test.Fatal(err)
			return
		}
		messageChan <- message
	}()
	for _, updatesChan := range updatesChans {
		ctx.areDomUpdatesEqual(security, <-updatesChan, snapshot)
	}

	for _, subscriber := range subscribers {
		subscriber.Close()
	}
	rpcExecWaiting.Wait()

}

////////////////////////////////////////////////////////////////////////////////

type marketDataServerContext struct {
	test     *testing.T
	ctrl     *gomock.Controller
	trekt    *mt.MockTrekt
	exchange *mt.MockMarketDataExchange
}

func createMarketDataServerContext(
	test *testing.T, name string) *marketDataServerContext {

	ctrl := gomock.NewController(test)
	result := &marketDataServerContext{
		test:     test,
		ctrl:     ctrl,
		trekt:    mt.NewMockTrekt(ctrl),
		exchange: mt.NewMockMarketDataExchange(ctrl)}

	result.trekt.EXPECT().LogDebugf(gomock.Any()).AnyTimes()
	result.trekt.EXPECT().LogDebugf(gomock.Any(), gomock.Any()).AnyTimes()
	result.trekt.EXPECT().LogDebugf(
		gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()
	result.trekt.EXPECT().LogDebugf(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()

	result.trekt.EXPECT().GetTypeName().Return(name).AnyTimes()

	result.exchange.EXPECT().GetTrekt().AnyTimes().Return(result.trekt)

	return result
}

func (ctx *marketDataServerContext) close() {
	ctx.ctrl.Finish()
}

// Test_MarketData_Server_Requests tests general requests getting workflow.
func Test_MarketData_Server_Requests(test *testing.T) {
	name := "Test node"
	ctx := createMarketDataServerContext(test, name)
	defer ctx.close()

	heartbeat := mt.NewMockMqHeartbeatClient(ctx.ctrl)
	heartbeatFailsChan := make(chan t.MqHeartbeatTestFail)
	defer close(heartbeatFailsChan)
	heartbeat.EXPECT().GetFailedTestsChan().AnyTimes().Return(heartbeatFailsChan)

	mqChannel := mt.NewMockMqChannel(ctx.ctrl)
	mqChannel.EXPECT().CreateHeartbeatClient().Return(heartbeat, nil).
		Do(func(...interface{}) { heartbeat.EXPECT().Close() })
	mqQueueName := "test queue"
	mqChannel.EXPECT().QueueDelete(mqQueueName, false, false, false).
		Return(0, nil).
		After(mqChannel.EXPECT().QueueDeclare("", false, true, true, false, nil).
			Return(amqp.Queue{Name: mqQueueName}, nil))
	mqChannel.EXPECT().QueueBind(mqQueueName, name, false, nil).Return(nil)
	requestsChan := make(chan amqp.Delivery)
	mqChannel.EXPECT().Consume(
		mqQueueName, gomock.Any(), true, false, false, false, nil).
		Do(
			func(string, consumer string, autoAck, exclusive, noLocal, noWait, args interface{}) {
				mqChannel.EXPECT().Cancel(consumer, false).
					Do(func(...interface{}) { close(requestsChan) }).
					Return(nil)
			}).
		Return(requestsChan, nil)

	server, err := t.CreateMarketDataServer(ctx.exchange, mqChannel)
	if err != nil || server == nil {
		test.Fatalf(`Failed to create server: "%s".`, err)
	}

	ctx.trekt.EXPECT().LogErrorf(
		`Failed to handle request to start market data "%s" from "%s": "%s".`,
		"Start error security", "Subscriber #1", errors.New("Start error"))
	ctx.trekt.EXPECT().LogErrorf(
		`Failed to handle request to stop market data "%s" from "%s": "%s".`,
		"Stop error security", "Subscriber #1", errors.New("Stop error"))

	ctx.trekt.EXPECT().LogErrorf(
		`Received request to stop market data for "%s", but it is not started.`,
		"Test security #0")
	ctx.trekt.EXPECT().LogErrorf(
		`Received request to stop market data for "%s", but it is not subscribed.`,
		"Test security #1")
	ctx.trekt.EXPECT().LogErrorf(
		`Received request to start market data for "%s"`+
			`, but it is already started for the subscriber.`,
		"Test security #2")

	ctx.trekt.EXPECT().LogInfof(
		`Market data for security "%s" is stopped.`, "Stop error security").
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Stop error security", "started", "Subscriber #1"))
	ctx.trekt.EXPECT().LogInfof(
		`Market data for security "%s" is %s by request from "%s".`,
		"Test security #1", "stopped", "Subscriber #2").
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #1", "started", "Subscriber #1"))
	ctx.trekt.EXPECT().LogInfof(
		`Market data for security "%s" is stopped.`, "Test security #2").
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #2", "started", "Subscriber #2"))

	numberOfRequests := 0
	isSecurity1Started := false
	isSecurity2Started := false
	go server.Handle(func(security string, isStart bool) error {
		numberOfRequests++
		if security == "Start error security" {
			return errors.New("Start error")
		} else if security == "Stop error security" {
			if isStart {
				return nil
			} else if numberOfRequests <= 3 {
				return errors.New("Stop error")
			} else {
				return nil
			}
		}
		var isStarted *bool
		if security == "Test security #1" {
			isStarted = &isSecurity1Started
		} else if security == "Test security #2" {
			isStarted = &isSecurity2Started
		}
		if isStarted == nil || isStart == *isStarted {
			ctx.test.Errorf(`Wrong request: "%s", %t, %v.`,
				security, isStart, isStarted)
		} else {
			*isStarted = isStart
		}
		return nil
	})

	send := func(request t.MarketDataRequest, correlation, subscriber int) {
		request.Subscriber = fmt.Sprintf("Subscriber #%d", subscriber)
		delivery := amqp.Delivery{
			CorrelationId: fmt.Sprintf("Correlation ID #%d", correlation),
			ReplyTo:       fmt.Sprintf("Reply address #%d", subscriber)}
		publishing := amqp.Publishing{
			CorrelationId: delivery.CorrelationId,
			ContentType:   "application/json"}
		publishing.Body, _ = json.Marshal(nil)
		mqChannel.EXPECT().Publish(delivery.ReplyTo, false, false, publishing)
		delivery.Body, _ = json.Marshal(&request)
		requestsChan <- delivery
	}
	send(
		t.MarketDataRequest{Security: "Start error security", IsStart: true}, 1, 1)
	send(
		t.MarketDataRequest{Security: "Stop error security", IsStart: true}, 1, 1)
	send(
		t.MarketDataRequest{Security: "Stop error security", IsStart: false}, 1, 1)
	send(t.MarketDataRequest{Security: "Test security #0", IsStart: false}, 9, 9)
	send(t.MarketDataRequest{Security: "Test security #1", IsStart: true}, 1, 1)
	send(t.MarketDataRequest{Security: "Test security #1", IsStart: true}, 2, 2)
	send(t.MarketDataRequest{Security: "Test security #2", IsStart: true}, 3, 2)
	send(t.MarketDataRequest{Security: "Test security #1", IsStart: false}, 4, 3)
	send(t.MarketDataRequest{Security: "Test security #1", IsStart: false}, 5, 1)
	send(t.MarketDataRequest{Security: "Test security #1", IsStart: false}, 6, 2)
	send(t.MarketDataRequest{Security: "Test security #2", IsStart: true}, 7, 2)

	server.Close()

	if numberOfRequests != 8 || isSecurity1Started || isSecurity2Started {
		ctx.test.Errorf("Wrong number of requests: %d, %t, %t.",
			numberOfRequests, isSecurity1Started, isSecurity2Started)
	}
}

// Test_MarketData_Server_Heartbeat tests how server stops market data, if
// requester is dead.
func Test_MarketData_Server_Heartbeat(test *testing.T) {
	name := "Test node"
	ctx := createMarketDataServerContext(test, name)
	defer ctx.close()

	heartbeat := mt.NewMockMqHeartbeatClient(ctx.ctrl)
	heartbeatFailsChan := make(chan t.MqHeartbeatTestFail)
	defer close(heartbeatFailsChan)
	heartbeat.EXPECT().GetFailedTestsChan().AnyTimes().Return(heartbeatFailsChan)

	mqChannel := mt.NewMockMqChannel(ctx.ctrl)
	mqChannel.EXPECT().CreateHeartbeatClient().Return(heartbeat, nil).
		Do(func(...interface{}) { heartbeat.EXPECT().Close() })
	mqQueueName := "test queue"
	mqChannel.EXPECT().QueueDelete(mqQueueName, false, false, false).
		Return(0, nil).
		After(mqChannel.EXPECT().QueueDeclare("", false, true, true, false, nil).
			Return(amqp.Queue{Name: mqQueueName}, nil))
	mqChannel.EXPECT().QueueBind(mqQueueName, name, false, nil).Return(nil)
	requestsChan := make(chan amqp.Delivery, 1)
	mqChannel.EXPECT().Consume(
		mqQueueName, gomock.Any(), true, false, false, false, nil).
		Do(
			func(string, consumer string, autoAck, exclusive, noLocal, noWait, args interface{}) {
				mqChannel.EXPECT().Cancel(consumer, false).
					Do(func(...interface{}) { close(requestsChan) }).
					Return(nil)
			}).
		Return(requestsChan, nil)

	server, err := t.CreateMarketDataServer(ctx.exchange, mqChannel)
	if err != nil || server == nil {
		test.Fatalf(`Failed to create server: "%s".`, err)
	}
	defer server.Close()

	heartbeatErr := errors.New("Test heartbeat error")

	ctx.trekt.EXPECT().LogInfof(
		`Stopping market data for "%d" by subscriber "%s" heartbeat error "%s"...`,
		"Test security #1", "Subscriber #1", heartbeatErr).
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #1", "started", "Subscriber #1"))
	ctx.trekt.EXPECT().LogInfof(
		`Stopping market data for "%d" by subscriber "%s" heartbeat error "%s"...`,
		"Test security #5", "Subscriber #1", heartbeatErr).
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #5", "started", "Subscriber #1"))

	ctx.trekt.EXPECT().LogInfof(`Market data for security "%s" is stopped.`,
		"Test security #3").
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #3", "started", "Subscriber #2"))
	ctx.trekt.EXPECT().LogInfof(`Market data for security "%s" is stopped.`,
		"Test security #4").
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #4", "started", "Subscriber #2"))

	ctx.trekt.EXPECT().LogInfof(`Market data for security "%s" is stopped.`,
		"Test security #2").
		After(ctx.trekt.EXPECT().LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			"Test security #2", "started", "Subscriber #1"))

	numberOfRequests := 0
	securitiesStates := map[string]bool{
		"Test security #1": false,
		"Test security #2": false,
		"Test security #3": false,
		"Test security #4": false,
		"Test security #5": false}
	startWaiting := sync.WaitGroup{}
	stopWaiting := sync.WaitGroup{}
	go server.Handle(func(security string, isStart bool) error {
		defer func() {
			if isStart {
				startWaiting.Done()
			} else if security == "Test security #1" ||
				security == "Test security #5" {

				stopWaiting.Done()
			}
		}()
		numberOfRequests++
		isStated, has := securitiesStates[security]
		if !has {
			ctx.test.Errorf(`Wrong security: "%s", %t.`, security, isStart)
			return nil
		}
		if isStated == isStart {
			ctx.test.Errorf(`Wrong state: "%s", %t.`, security, isStart)
		}
		securitiesStates[security] = isStart
		return nil
	})

	send := func(security int, subscriber int) {
		request := t.MarketDataRequest{
			Security:   fmt.Sprintf("Test security #%d", security),
			IsStart:    true,
			Subscriber: fmt.Sprintf("Subscriber #%d", subscriber)}
		delivery := amqp.Delivery{
			CorrelationId: "Correlation ID #X",
			ReplyTo:       "Reply address #X"}
		publishing := amqp.Publishing{
			CorrelationId: delivery.CorrelationId,
			ContentType:   "application/json"}
		publishing.Body, _ = json.Marshal(nil)
		mqChannel.EXPECT().Publish(delivery.ReplyTo, false, false, publishing)
		delivery.Body, _ = json.Marshal(&request)
		requestsChan <- delivery
	}
	startWaiting.Add(len(securitiesStates))
	send(1, 1)
	send(3, 2)
	send(2, 1)
	send(2, 3)
	send(4, 2)
	send(5, 1)
	startWaiting.Wait()

	stopWaiting.Add(2)
	heartbeatFailsChan <- t.MqHeartbeatTestFail{
		Err: errors.New("XXX"), Address: "Test security XXX"}
	heartbeatFailsChan <- t.MqHeartbeatTestFail{
		Err: heartbeatErr, Address: "Subscriber #1"}
	stopWaiting.Wait()

	if numberOfRequests != 7 ||
		securitiesStates["Test security #1"] ||
		!securitiesStates["Test security #2"] ||
		!securitiesStates["Test security #3"] ||
		!securitiesStates["Test security #4"] ||
		securitiesStates["Test security #5"] {

		ctx.test.Errorf("Wrong number of requests: %d, %v.",
			numberOfRequests, securitiesStates)
	}

}

////////////////////////////////////////////////////////////////////////////////
