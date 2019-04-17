// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/rektra-network/trekt-go/pkg/tradinglib"
	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// MarketDataExchange represents market data message exchange.
type MarketDataExchange interface {
	// Close closes the exchange.
	Close()
	// GetTrekt returns TREKT.
	GetTrekt() Trekt
	// CreateServer creates a market data server to handle market data requests.
	CreateServer() (*MarketDataServer, error)
	// CreateServerOrExit creates an to handle market data requests or exits
	// with error printing if creating is failed.
	CreateServerOrExit() *MarketDataServer
	// CreateService creates a market data service to receive market data.
	CreateService(capacity uint16) (*MarketDataService, error)
	// CreateServiceOrExit creates a market data service to receive market data or
	// exits with error printing if creating is failed.
	CreateServiceOrExit(capacity uint16) *MarketDataService
}

type marketDataExchange struct {
	mq     mqChannel
	stream streamChannel
}

func createMarketDataExchange(
	trekt Trekt,
	mq *Mq,
	stream *Stream,
	capacity uint16) (MarketDataExchange, error) {

	result := &marketDataExchange{}
	if err := result.mq.init("md", "topic", trekt, mq, capacity); err != nil {
		return nil, err
	}
	if err := result.stream.init(stream, trekt); err != nil {
		return nil, err
	}
	return result, nil
}

func (exchange *marketDataExchange) Close() {
	exchange.stream.Close()
	exchange.mq.Close()
}

func (exchange *marketDataExchange) GetTrekt() Trekt {
	return exchange.mq.trekt
}

func (exchange *marketDataExchange) CreateServer() (*MarketDataServer, error) {
	return CreateMarketDataServer(exchange, &exchange.mq)
}

func (exchange *marketDataExchange) CreateServerOrExit() *MarketDataServer {
	result, err := exchange.CreateServer()
	if err != nil {
		exchange.GetTrekt().LogErrorf(`Failed to create market data server: "%s".`,
			err)
		os.Exit(1)
	}
	return result
}

func (exchange *marketDataExchange) CreateService(capacity uint16) (
	*MarketDataService, error) {

	return CreateMarketDataService(
		exchange, &exchange.mq, &exchange.stream, capacity)
}

func (exchange *marketDataExchange) CreateServiceOrExit(
	capacity uint16) *MarketDataService {

	result, err := exchange.CreateService(capacity)
	if err != nil {
		exchange.GetTrekt().LogErrorf(`Failed to create market data service: "%s".`,
			err)
		os.Exit(1)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////

// MarketDataRequest describes internal package request to start or to stop
// market data.
type MarketDataRequest struct {
	Security   string
	IsStart    bool
	Subscriber string
}

// MarketDataStartRequestResponse describes internal package start request
// response.
type MarketDataStartRequestResponse struct {
	Snapshot DepthOfMarketLevels
	Start    int64
}

// DepthOfMarketLevel describes a level of depth of market.
type DepthOfMarketLevel []float64

// Clone makes level clone.
func (level DepthOfMarketLevel) Clone() DepthOfMarketLevel {
	return DepthOfMarketLevel{level[0], level[1]}
}

// GetKey returns level key.
func (level DepthOfMarketLevel) GetKey() float64 { return level[0] }

// Set sets level data.
func (level *DepthOfMarketLevel) Set(price, volume float64, isBid bool) {
	(*level)[0] = price
	if isBid {
		(*level)[0] *= -1
	}
	(*level)[1] = volume
}

// GetPrice returns level price.
func (level DepthOfMarketLevel) GetPrice() float64 { return math.Abs(level[0]) }

// GetQty returns level volume.
func (level DepthOfMarketLevel) GetQty() float64 { return level[1] }

// IsDeleted returns true if level is deleted.
func (level DepthOfMarketLevel) IsDeleted() bool {
	return level.GetQty() == 0
}

// SetDeleted marks records as "deleted".
func (level DepthOfMarketLevel) SetDeleted() {
	level[1] = 0
}

// IsBid returns true if level is bid.
func (level DepthOfMarketLevel) IsBid() bool { return level[0] <= 0 }

// DepthOfMarketLevels describes set of levels  of depth of market.
type DepthOfMarketLevels []DepthOfMarketLevel

// Clone makes levels clone.
func (levels DepthOfMarketLevels) Clone() DepthOfMarketLevels {
	result := make(DepthOfMarketLevels, len(levels))
	for i, level := range levels {
		result[i] = level.Clone()
	}
	return result
}

// DepthOfMarketUpdate describes depth of market update.
type DepthOfMarketUpdate struct {
	Security tradinglib.Security
	Levels   DepthOfMarketLevels
}

///////////////////////////////////////////////////////////////////////////////

// MarketDataServer represents server which provides a market data by requests.
type MarketDataServer struct {
	mqRPCServer
	subscribers map[string]map[string]interface{}
	requestChan chan MarketDataRequest
	handle      func(securityID string, isStart bool) error
	stopWaiting sync.WaitGroup
}

// CreateMarketDataServer creates MarketDataServer instance.
func CreateMarketDataServer(
	exchange MarketDataExchange, mq MqChannel) (*MarketDataServer, error) {

	result := &MarketDataServer{}
	trekt := exchange.GetTrekt()
	err := result.mqRPCServer.init(trekt.GetTypeName(), mq, trekt)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the server.
func (server *MarketDataServer) Close() {
	server.mqRPCServer.close()
	server.stopWaiting.Wait()
}

// Handle accepts market data requests and calls a handler for each.
func (server *MarketDataServer) Handle(
	handle func(security string, isStart bool) error) {

	server.stopWaiting.Add(1)
	defer server.stopWaiting.Done()

	server.requestChan = make(chan MarketDataRequest, 1)
	go func() {
		defer close(server.requestChan)
		server.mqRPCServer.handle(
			func(requestMessage amqp.Delivery) (interface{}, error) {
				request := MarketDataRequest{}
				err := json.Unmarshal(requestMessage.Body, &request)
				if err != nil {
					server.trekt.LogErrorf(
						`Failed to parse market data request "%s" from "%s": "%s".`,
						string(requestMessage.Body), requestMessage.ReplyTo, err)
					return nil, errors.New("Internal error")
				}
				server.requestChan <- request
				return nil, nil
			})
	}()

	server.subscribers = map[string]map[string]interface{}{}
	defer func() {
		if len(server.subscribers) == 0 {
			return
		}
		server.trekt.LogDebugf(
			"Stopping market data for %d securities"+
				" due to the handling process is stopped.",
			len(server.subscribers))
		for security := range server.subscribers {
			if err := handle(security, false); err != nil {
				server.trekt.LogErrorf(
					`Failed to stop market data for "%d"`+
						` by handling process is stopping: "%s".`,
					security, err)
			}
			server.trekt.LogInfof(`Market data for security "%s" is stopped.`,
				security)
		}
	}()

	hearbeat, err := server.channel.CreateHeartbeatClient()
	if err != nil {
		server.trekt.LogErrorf(`Failed to start hearbeat client: "%s".`, err)
	} else {
		defer hearbeat.Close()
	}

	server.handle = handle
	for {
		select {
		case request, isOpened := <-server.requestChan:
			if !isOpened {
				return
			}
			server.handleRequest(request)
		case fail := <-hearbeat.GetFailedTestsChan():
			server.stopByDisconnectedSubscribers(fail.Address, fail.Err)
			break
		}
	}
}

func (server *MarketDataServer) handleRequest(request MarketDataRequest) {

	if subscribers, isStarted := server.subscribers[request.Security]; !isStarted {

		if !request.IsStart {
			server.trekt.LogErrorf(
				`Received request to stop market data for "%s", but it is not started.`,
				request.Security)
			return
		}

		if err := server.handle(request.Security, request.IsStart); err != nil {
			server.trekt.LogErrorf(
				`Failed to handle request to start market data "%s" from "%s": "%s".`,
				request.Security, request.Subscriber, err)
			return
		}
		subscribers = map[string]interface{}{request.Subscriber: nil}
		server.subscribers[request.Security] = subscribers

	} else {

		_, isSubscribed := subscribers[request.Subscriber]

		if request.IsStart {
			if isSubscribed {
				server.trekt.LogErrorf(
					`Received request to start market data for "%s"`+
						`, but it is already started for the subscriber.`,
					request.Security)
				return
			}
			subscribers[request.Subscriber] = nil
			return
		} else if !isSubscribed {
			server.trekt.LogErrorf(
				`Received request to stop market data for "%s"`+
					`, but it is not subscribed.`,
				request.Security)
			return
		} else {
			if len(subscribers) == 1 {
				if err := server.handle(request.Security, request.IsStart); err != nil {
					server.trekt.LogErrorf(
						`Failed to handle request to stop market data "%s" from "%s":`+
							` "%s".`,
						request.Security, request.Subscriber, err)
					return
				}
			}
			delete(subscribers, request.Subscriber)
			if len(subscribers) > 0 {
				return
			}
			delete(server.subscribers, request.Security)
		}
	}

	{
		var commandName string
		if !request.IsStart {
			commandName = "stopped"
		} else {
			commandName = "started"
		}
		server.trekt.LogInfof(
			`Market data for security "%s" is %s by request from "%s".`,
			request.Security, commandName, request.Subscriber)
	}

}

func (server *MarketDataServer) stopByDisconnectedSubscribers(
	subscriber string,
	err error) {

	for securityID, security := range server.subscribers {
		if _, has := security[subscriber]; !has {
			continue
		}
		if len(security) > 1 {
			delete(security, subscriber)
			continue
		}
		server.trekt.LogInfof(
			`Stopping market data for "%d" by subscriber "%s" heartbeat error "%s"...`,
			securityID, subscriber, err)
		if err := server.handle(securityID, false); err != nil {
			server.trekt.LogErrorf(`Failed to stop market data for "%d"`+
				` by subscriber "%s" disconnection: "%s".`,
				securityID, subscriber, err)
			continue
		}
		delete(server.subscribers, securityID)
	}
}

///////////////////////////////////////////////////////////////////////////////

type marketDataStreamMessage struct {
	subscription *marketDataStreamSubscription
	update       sarama.ConsumerMessage
}
type marketDataStreamSubscription struct {
	security     tradinglib.Security
	subscription Subscription
	snapshot     map[float64]DepthOfMarketLevel
	channels     *map[chan<- DepthOfMarketUpdate]*uint
}

func createMarketDataStreamSubscription(security tradinglib.Security) (
	*marketDataStreamSubscription, error) {

	channels := map[chan<- DepthOfMarketUpdate]*uint{}
	result := &marketDataStreamSubscription{
		security: security, channels: &channels}
	return result, nil
}

func (subscription *marketDataStreamSubscription) close() error {
	if subscription.subscription == nil {
		return nil
	}
	return subscription.subscription.Close()
}

func (subscription *marketDataStreamSubscription) start(
	startRequestResponse MarketDataStartRequestResponse,
	sourceChannel StreamChannel,
	updatesChan chan<- marketDataStreamMessage,
	updatesChanCloseWaiting *sync.WaitGroup) error {

	subscription.snapshot = map[float64]DepthOfMarketLevel{}
	for _, level := range startRequestResponse.Snapshot {
		subscription.snapshot[level.GetKey()] = level
	}

	var err error
	subscription.subscription, err = sourceChannel.CreateSubscription("md",
		startRequestResponse.Start)
	if err != nil {
		return err
	}

	go func() {
		updatesChanCloseWaiting.Add(1)
		defer updatesChanCloseWaiting.Done()
		for {
			message, isOpened := <-subscription.subscription.Messages()
			if !isOpened {
				return
			}
			updatesChan <- marketDataStreamMessage{
				subscription: subscription, update: message}
		}
	}()

	return nil
}

// MarketDataService represents service which accepts market data requests and
// provides market data requests by these requests.
type MarketDataService struct {
	exchange     MarketDataExchange
	rpc          RPCClient
	stopChan     chan interface{}
	stopBarrier  sync.WaitGroup
	requestsChan chan struct {
		subscription *MarketDataSubscription
		isStart      bool
		waiting      *sync.WaitGroup
	}
	startChan chan struct {
		security tradinglib.Security
		response MarketDataStartRequestResponse
	}

	heartbeat MqHeartbeatServer

	requestSet              map[tradinglib.SecurityKey]*marketDataStreamSubscription
	updatesChan             chan marketDataStreamMessage
	updatesChanCloseWaiting sync.WaitGroup
	sourceChannel           StreamChannel
}

// CreateMarketDataService creates MarketDataService instance.
func CreateMarketDataService(
	exchange MarketDataExchange,
	mq MqChannel,
	sourceChannel StreamChannel,
	capacity uint16) (*MarketDataService, error) {

	result := &MarketDataService{
		exchange: exchange,
		stopChan: make(chan interface{}),
		requestsChan: make(chan struct {
			subscription *MarketDataSubscription
			isStart      bool
			waiting      *sync.WaitGroup
		}, capacity),
		startChan: make(chan struct {
			security tradinglib.Security
			response MarketDataStartRequestResponse
		}, 1),
		sourceChannel: sourceChannel}

	var err error

	result.rpc, err = mq.CreateRPCClient()
	if err != nil {
		close(result.startChan)
		close(result.requestsChan)
		return nil, err
	}

	go func() {
		result.stopBarrier.Add(1)
		defer result.stopBarrier.Done()
		{
			var err error
			result.heartbeat, err = mq.CreateHeartbeatServer()
			if err != nil {
				result.exchange.GetTrekt().LogErrorf(
					`Failed to start heartbeat server: "%s".`, err)
				return
			}
			defer result.heartbeat.Close()
		}
		result.run(capacity)
	}()

	return result, nil
}

// Close stops the service.
func (service *MarketDataService) Close() {
	service.stopChan <- nil
	service.stopBarrier.Wait()
	service.rpc.Close()
}

// StartDepthOfMarket requests a depth of market updates streaming start.
func (service *MarketDataService) StartDepthOfMarket(
	security tradinglib.Security,
	updatesChan chan<- DepthOfMarketUpdate) (*MarketDataSubscription, error) {

	result := &MarketDataSubscription{
		service:     service,
		security:    security,
		updatesChan: updatesChan}
	service.requestsChan <- struct {
		subscription *MarketDataSubscription
		isStart      bool
		waiting      *sync.WaitGroup
	}{subscription: result, isStart: true}
	return result, nil
}

func (service *MarketDataService) run(capacity uint16) {
	defer close(service.startChan)
	defer close(service.requestsChan)

	service.updatesChan = make(chan marketDataStreamMessage, capacity+1)
	service.updatesChanCloseWaiting.Add(1)
	go func() {
		defer close(service.updatesChan)
		service.updatesChanCloseWaiting.Wait()
	}()

	service.requestSet =
		map[tradinglib.SecurityKey]*marketDataStreamSubscription{}

	for {
		select {
		case update, isOpened := <-service.updatesChan:
			if !isOpened {
				return
			}
			service.broadcastUpdate(update)
		case response := <-service.startChan:
			service.startSubscription(response.security, response.response)
		case request := <-service.requestsChan:
			if request.waiting != nil {
				request.waiting.Done()
			}
			if request.isStart {
				service.requestSubscriptionStart(request.subscription)
			} else {
				service.requestSubscriptionStop(request.subscription)
			}
		case <-service.stopChan:
			service.updatesChanCloseWaiting.Done()
			if len(service.requestSet) == 0 {
				break
			}
			service.exchange.GetTrekt().LogDebugf(
				`Stopping market data for %d securities as service is stopped...`,
				len(service.requestSet))
			for _, subscription := range service.requestSet {
				service.sendStopRequest(subscription.security)
				if err := subscription.close(); err != nil {
					service.exchange.GetTrekt().LogErrorf(
						`Failed to close market data stream subscription: "%s".`, err)
				}
			}
		}
	}

}

func (service *MarketDataService) broadcast(
	subscription *marketDataStreamSubscription,
	update DepthOfMarketUpdate) {

	for channel := range *subscription.channels {
		channel <- update
	}
}

func (service *MarketDataService) broadcastSnapshot(
	subscription *marketDataStreamSubscription) {

	if len(subscription.snapshot) == 0 {
		return
	}
	snapshot := DepthOfMarketUpdate{
		Security: subscription.security,
		Levels:   make(DepthOfMarketLevels, len(subscription.snapshot))}
	i := 0
	for _, level := range subscription.snapshot {
		snapshot.Levels[i] = level
		i++
	}
	service.broadcast(subscription, snapshot)
}

func (service *MarketDataService) broadcastUpdate(
	message marketDataStreamMessage) {

	update := DepthOfMarketUpdate{Security: message.subscription.security}
	if err := json.Unmarshal(message.update.Value, &update.Levels); err != nil {
		log.Fatalf(`Failed to parse market data update message: "%s".`, err)
		return
	}
	if len(update.Levels) != 0 {
		// Incremental update.
		for _, level := range update.Levels {
			if level.IsDeleted() {
				delete(message.subscription.snapshot, level.GetKey())
			} else {
				message.subscription.snapshot[level.GetKey()] = level
			}
		}
	} else {
		// Empty packet means "from the scratch".
		if len(message.subscription.snapshot) == 0 {
			return
		}
		for _, level := range message.subscription.snapshot {
			level.SetDeleted()
			update.Levels = append(update.Levels, level)
		}
		message.subscription.snapshot = map[float64]DepthOfMarketLevel{}
	}
	service.broadcast(message.subscription, update)
}

func (service *MarketDataService) startSubscription(
	security tradinglib.Security,
	response MarketDataStartRequestResponse) {

	subscription, isRequested := service.requestSet[security.GetKey()]
	if !isRequested {
		return
	}
	err := subscription.start(
		response,
		service.sourceChannel,
		service.updatesChan,
		&service.updatesChanCloseWaiting)
	if err != nil {
		service.exchange.GetTrekt().LogErrorf(
			`Failed to start market data stream subscription "%s" on "%s": "%s".`,
			security.ID, security.Exchange, err)
		return
	}
	service.broadcastSnapshot(subscription)
}

func (service *MarketDataService) requestSubscriptionStart(
	request *MarketDataSubscription) {

	subscription, isRequested := service.requestSet[request.security.GetKey()]
	if !isRequested {
		var err error
		subscription, err = createMarketDataStreamSubscription(request.security)
		if err != nil {
			service.exchange.GetTrekt().LogErrorf(
				`Failed to create market data stream subscription "%s" on "%s": "%s".`,
				request.security.ID, request.security.Exchange, err)
			return
		}
		service.requestSet[request.security.GetKey()] = subscription
		service.sendStartRequest(request.security)
	}

	refCounter := (*subscription.channels)[request.updatesChan]
	if refCounter == nil {
		refCounterVar := uint(1)
		(*subscription.channels)[request.updatesChan] = &refCounterVar
		service.broadcastSnapshot(subscription)
	} else {
		(*refCounter)++
	}
}

func (service *MarketDataService) requestSubscriptionStop(
	request *MarketDataSubscription) {

	subscription, isRequested := service.requestSet[request.security.GetKey()]
	if !isRequested {
		service.exchange.GetTrekt().LogWarnf(
			`Received request to stop market data by unknown subscription`+
				` "%s" on "%s".`,
			request.security.ID, request.security.Exchange)
		return
	}

	numberOfRefs, isSubscribed := (*subscription.channels)[request.updatesChan]
	if !isSubscribed {
		service.exchange.GetTrekt().LogErrorf(
			"Received request to stop market data by not subscribed channel"+
				` "%s" on "%s".`,
			request.security.ID, request.security.Exchange)
		return
	}

	(*numberOfRefs)--
	if *numberOfRefs > 0 {
		return
	}

	delete(*subscription.channels, request.updatesChan)
	if len(*subscription.channels) > 0 {
		return
	}

	service.sendStopRequest(request.security)
	delete(service.requestSet, request.security.GetKey())
	if err := subscription.close(); err != nil {
		service.exchange.GetTrekt().LogErrorf(
			`Failed to close market data stream subscription: "%s".`, err)
	}
}

func (service *MarketDataService) sendStartRequest(
	security tradinglib.Security) {

	service.exchange.GetTrekt().LogDebugf(
		`Requesting to start market data for "%s" on "%s"...`,
		security.ID, security.Exchange)
	service.rpc.Request(
		security.Exchange, // routing key
		true,              // mandatory
		MarketDataRequest{
			Security:   security.ID,
			IsStart:    true,
			Subscriber: service.heartbeat.GetAddress()},
		func(response []byte) {
			startResponse := MarketDataStartRequestResponse{}
			if err := json.Unmarshal(response, &startResponse); err != nil {
				service.exchange.GetTrekt().LogErrorf(
					`Failed to parse market data start request response: "%s".`+
						" Message: %s.",
					err, response)
				return
			}
			service.exchange.GetTrekt().LogInfof(
				`Market data for "%s" on "%s" is started from update %d.`,
				security.ID, security.Exchange, startResponse.Start)
			service.startChan <- struct {
				security tradinglib.Security
				response MarketDataStartRequestResponse
			}{security: security, response: startResponse}
		},
		func(err error) {
			service.exchange.GetTrekt().LogErrorf(
				`Failed to start market data for "%s" on "%s": "%s".`,
				security.ID, security.Exchange, err)
		})
}

func (service *MarketDataService) sendStopRequest(
	security tradinglib.Security) {

	service.exchange.GetTrekt().LogDebugf(
		`Requesting to stop market data for "%s" on "%s"...`,
		security.ID, security.Exchange)
	service.rpc.Request(
		security.Exchange, // routing key
		true,              // mandatory
		MarketDataRequest{
			Security:   security.ID,
			IsStart:    false,
			Subscriber: service.heartbeat.GetAddress()},
		func([]byte) {
			service.exchange.GetTrekt().LogInfof(
				`Market data for "%s" on "%s" is stopped.`,
				security.ID, security.Exchange)
		},
		func(err error) {
			service.exchange.GetTrekt().LogErrorf(
				`Failed to stop market data for "%s" on "%s": "%s".`,
				security.ID, security.Exchange, err)
		})
}

///////////////////////////////////////////////////////////////////////////////

// MarketDataSubscription represents subscription at one security on one
// market data source.
type MarketDataSubscription struct {
	id          uint64
	service     *MarketDataService
	security    tradinglib.Security
	updatesChan chan<- DepthOfMarketUpdate
}

// Close closes the subscription.
func (subscription *MarketDataSubscription) Close() error {
	waiting := sync.WaitGroup{}
	waiting.Add(1)
	subscription.service.requestsChan <- struct {
		subscription *MarketDataSubscription
		isStart      bool
		waiting      *sync.WaitGroup
	}{subscription: subscription, isStart: false, waiting: &waiting}
	waiting.Wait()
	return nil
}

///////////////////////////////////////////////////////////////////////////////
