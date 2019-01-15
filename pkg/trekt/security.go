// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rektra-network/trekt-go/pkg/tradinglib"
	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// SecurityState describes security state.
type SecurityState struct {
	Security tradinglib.Security
	IsActive *bool
}

// SecurityStateList is a list of security states.
type SecurityStateList = []SecurityState

///////////////////////////////////////////////////////////////////////////////

type securitySymbolStateMessage struct {
	ID       string
	Symbol   interface{}
	Type     string
	IsActive *bool
}
type securityStateListMessage = []securitySymbolStateMessage

///////////////////////////////////////////////////////////////////////////////

// SecuritiesExchange represents security states exchange.
type SecuritiesExchange struct{ mqExchange }

func createSecuritiesExchange(
	trekt *Trekt, capacity uint16) (*SecuritiesExchange, error) {

	result := &SecuritiesExchange{}
	err := result.mqExchange.init("securities", "topic", trekt, capacity)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the exchange.
func (exchange *SecuritiesExchange) Close() {
	exchange.mqExchange.close()
}

// CreateServer creates a server which provides the information about
// securities.
func (exchange *SecuritiesExchange) CreateServer() (*SecuritiesServer, error) {
	return createSecuritiesServer(exchange)
}

// CreateServerOrExit creates a server which provides the information about
// securities or exits with error printing if creating is failed.
func (exchange *SecuritiesExchange) CreateServerOrExit() *SecuritiesServer {
	result, err := exchange.CreateServer()
	if err != nil {
		exchange.trekt.LogErrorf(`Failed to create securities server: "%s".`,
			err)
		os.Exit(1)
	}
	return result
}

// CreateSubscription creates a subscription which allows to receive information
// about security list.
func (exchange *SecuritiesExchange) CreateSubscription(
	capacity uint16) (*SecuritiesSubscription, error) {

	return createSecuritiesSubscription(exchange, capacity)
}

// CreateSubscriptionOrExit creates a subscription which allows to receive
// information about security list or exits with error printing if creating
// is failed.
func (exchange *SecuritiesExchange) CreateSubscriptionOrExit(
	capacity uint16) *SecuritiesSubscription {

	result, err := exchange.CreateSubscription(capacity)
	if err != nil {
		exchange.trekt.LogErrorf(`Failed to create securities subscription: "%s".`,
			err)
		os.Exit(1)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////

// SecuritiesServer represents a server that holds securities list and provides
// information about this list.
type SecuritiesServer struct {
	exchange   *SecuritiesExchange
	securities map[string]*map[string]securitySymbolStateMessage
	heartbeat  mqRPCServer
}

func createSecuritiesServer(
	exchange *SecuritiesExchange) (*SecuritiesServer, error) {
	result := &SecuritiesServer{exchange: exchange}
	return result, nil
}

// Close closes the server.
func (server *SecuritiesServer) Close() {}

// RunOrExit runs a server which reads security information from a channel and
// provides the information for a network. RunOrExit stops process at error.
func (server *SecuritiesServer) RunOrExit(
	updatesChan <-chan SecurityStateList) {

	err := server.Run(updatesChan)
	if err != nil {
		server.exchange.trekt.LogErrorf(`Failed to run securities server: "%s".`,
			err)
		os.Exit(1)
	}
}

// Run runs a server which reads security information from a channel and
// provides the information for a network.
func (server *SecuritiesServer) Run(
	updatesChan <-chan SecurityStateList) error {

	requestChan := make(chan string)
	defer close(requestChan)
	responseChan := make(chan map[string]securityStateListMessage)
	defer close(responseChan)

	err := server.heartbeat.init(
		"", // query
		&server.exchange.mqExchange)
	if err != nil {
		return err
	}
	defer server.heartbeat.close()
	go server.heartbeat.handle(func(request amqp.Delivery) (interface{}, error) {
		return struct{}{}, nil
	})

	var snapshotServer *mqSubscription
	snapshotServer, err = createMqSubscription(
		"*.request", // query
		&server.exchange.mqExchange,
		true) // auto-ack
	if err != nil {
		return err
	}
	defer snapshotServer.close()
	go snapshotServer.handle(func(request amqp.Delivery) {
		pos := strings.Index(request.RoutingKey, ".")
		requestChan <- request.RoutingKey[:pos]
		response, isOpened := <-responseChan
		if !isOpened {
			return
		}
		message := amqp.Publishing{
			ReplyTo:     server.heartbeat.getReplyName(),
			ContentType: "application/json"}
		var err error
		message.Body, err = json.Marshal(response)
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to serialize security state list: "%s".`, err)
			return
		}
		err = snapshotServer.exchange.publish(
			request.ReplyTo,
			false, // mandatory
			false, // immediate
			message)
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to publish security state list: "%s".`, err)
		}

	})

	server.securities = make(map[string]*map[string]securitySymbolStateMessage)
	defer server.unregisterAll()
	for {
		select {
		case update, isOpened := <-updatesChan:
			if !isOpened {
				return nil
			}
			server.broadcast(server.merge(update))
		case request := <-requestChan:
			if request == "*" {
				responseChan <- server.dumpAll()
			} else {
				responseChan <- server.dumpExchange(request)
			}
		}
	}

}

func (server *SecuritiesServer) merge(
	update SecurityStateList) map[string]*map[string]securitySymbolStateMessage {

	result := make(map[string]*map[string]securitySymbolStateMessage)

	for _, security := range update {
		message := securitySymbolStateMessage{
			ID:       security.Security.ID,
			Symbol:   security.Security.Symbol.Export(),
			Type:     security.Security.Symbol.GetType(),
			IsActive: security.IsActive}

		{
			list, has := result[security.Security.Exchange]
			if !has {
				newNode := make(map[string]securitySymbolStateMessage)
				list = &newNode
				result[security.Security.Exchange] = list
			}
			(*list)[security.Security.ID] = message
		}

		if security.IsActive == nil {
			if _, has := server.securities[security.Security.Exchange]; has {
				delete(*server.securities[security.Security.Exchange],
					security.Security.ID)
			}
		} else {
			exchange, has := server.securities[security.Security.Exchange]
			if !has {
				newExchange := make(map[string]securitySymbolStateMessage)
				exchange = &newExchange
				server.securities[security.Security.Exchange] = exchange
			}
			(*exchange)[security.Security.ID] = message
		}

	}

	return result
}

func (server *SecuritiesServer) dumpAll() map[string]securityStateListMessage {
	result := make(map[string]securityStateListMessage)
	for exchange, securities := range server.securities {
		list := make(securityStateListMessage, len(*securities))
		i := 0
		for _, security := range *securities {
			list[i] = security
			i++
		}
		result[exchange] = list
	}
	return result
}

func (server *SecuritiesServer) dumpExchange(
	exchange string) map[string]securityStateListMessage {

	result := make(map[string]securityStateListMessage)
	if securities, has := server.securities[exchange]; has {
		list := make(securityStateListMessage, len(*securities))
		i := 0
		for _, security := range *securities {
			list[i] = security
			i++
		}
		result[exchange] = list
	}
	return result
}

func (server *SecuritiesServer) broadcast(
	source map[string]*map[string]securitySymbolStateMessage) {

	for exchange, securities := range source {
		list := make(securityStateListMessage, len(*securities))
		i := 0
		for _, security := range *securities {
			list[i] = security
			i++
		}
		message := amqp.Publishing{
			ReplyTo:     server.heartbeat.getReplyName(),
			ContentType: "application/json"}
		var err error
		message.Body, err = json.Marshal(list)
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to serialize security state list: "%s".`, err)
			continue
		}
		err = server.exchange.publish(
			exchange+".update",
			false, // mandatory
			false, // immediate
			message)
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to publish security state list: "%s".`, err)
		}
	}
}

func (server *SecuritiesServer) unregisterAll() {
	if len(server.securities) == 0 {
		return
	}
	server.exchange.trekt.LogInfof(
		"Removing securities from %d exchanges"+
			" due to the registration process is stopped...",
		len(server.securities))
	for _, securities := range server.securities {
		for id, security := range *securities {
			security.IsActive = nil
			(*securities)[id] = security
		}
	}
	server.broadcast(server.securities)
	server.securities = make(map[string]*map[string]securitySymbolStateMessage)
}

///////////////////////////////////////////////////////////////////////////////

// SecuritiesSubscriptionNotificationID represents ID of notification
// subscription.
type SecuritiesSubscriptionNotificationID = uint64

// SecuritiesSubscriptionNotificationSubscriber allows creating security update
// subscription.
type SecuritiesSubscriptionNotificationSubscriber struct{}

// SecuritiesSubscription represents subscription to security lists changes.
type SecuritiesSubscription struct {
	mqSubscription
	rpc mqRPCClient

	updatesChan chan struct {
		update           securityStateListMessage
		exchange, source string
	}

	snapshotsChan chan struct {
		snapshot map[string]securityStateListMessage
		source   string
	}
	snapshotsSubscription *mqSubscription

	sources map[string]*map[string]*struct {
		state   SecurityState
		sources map[string]struct{}
	}
	securities map[string]*map[string]*struct {
		state   SecurityState
		sources map[string]struct{}
	}

	snapshotRequestsChan chan func(SecurityStateList)

	prevNotificationID SecuritiesSubscriptionNotificationID
	notifyRequestsChan chan struct {
		id         SecuritiesSubscriptionNotificationID
		notifyChan chan SecurityStateList
	}
	notifyCancelRequestChan chan SecuritiesSubscriptionNotificationID
	notifyChannels          map[SecuritiesSubscriptionNotificationID]chan SecurityStateList
}

func createSecuritiesSubscription(
	exchange *SecuritiesExchange,
	clientsCapacity uint16) (*SecuritiesSubscription, error) {

	result := &SecuritiesSubscription{
		sources: make(map[string]*map[string]*struct {
			state   SecurityState
			sources map[string]struct{}
		}),
		securities: make(map[string]*map[string]*struct {
			state   SecurityState
			sources map[string]struct{}
		})}

	result.updatesChan = make(chan struct {
		update           securityStateListMessage
		exchange, source string
	}, 1)
	err := result.mqSubscription.init(
		"*.update", // query
		result.exchange,
		true) // is auto-ack
	if err != nil {
		close(result.updatesChan)
		return nil, err
	}
	go result.handle(func(message amqp.Delivery) {
		update := struct {
			update           securityStateListMessage
			exchange, source string
		}{
			update:   securityStateListMessage{},
			exchange: message.RoutingKey[:strings.Index(message.RoutingKey, ".")],
			source:   message.ReplyTo}
		err := json.Unmarshal(message.Body, &update.update)
		if err != nil {
			result.exchange.trekt.LogErrorf(
				`Failed to parse security list update: "%s".`, err)
		}
		result.updatesChan <- update
	})

	err = result.rpc.init(result.exchange)
	if err != nil {
		close(result.snapshotsChan)
		result.mqSubscription.close()
		close(result.updatesChan)
		return nil, err
	}

	result.snapshotsChan = make(chan struct {
		snapshot map[string]securityStateListMessage
		source   string
	}, 1)
	result.snapshotsSubscription, err = createMqSubscription(
		"", // query
		result.exchange,
		true) // auto-ack
	if err != nil {
		result.rpc.close()
		close(result.snapshotsChan)
		result.mqSubscription.close()
		close(result.updatesChan)
		return nil, err
	}
	go result.snapshotsSubscription.handle(func(message amqp.Delivery) {
		snapshot := struct {
			snapshot map[string]securityStateListMessage
			source   string
		}{
			snapshot: map[string]securityStateListMessage{},
			source:   message.ReplyTo}
		err := json.Unmarshal(message.Body, &snapshot.snapshot)
		if err != nil {
			result.exchange.trekt.LogErrorf(
				`Failed to parse security list snapshot: "%s".`,
				err)
		}
		result.snapshotsChan <- snapshot
	})
	err = result.exchange.publish(
		"*.request", // key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{ReplyTo: result.snapshotsSubscription.queue.Name})
	if err != nil {
		result.snapshotsSubscription.close()
		close(result.snapshotsChan)
		result.rpc.close()
		result.mqSubscription.close()
		close(result.updatesChan)
		return nil, err
	}

	result.snapshotRequestsChan = make(
		chan func(SecurityStateList), clientsCapacity)

	result.notifyRequestsChan = make(
		chan struct {
			id         SecuritiesSubscriptionNotificationID
			notifyChan chan SecurityStateList
		}, clientsCapacity)
	result.notifyCancelRequestChan = make(
		chan SecuritiesSubscriptionNotificationID, clientsCapacity)
	result.notifyChannels =
		map[SecuritiesSubscriptionNotificationID]chan SecurityStateList{}

	go result.run()

	return result, nil
}

// Close closes the subscription.
func (subscription *SecuritiesSubscription) Close() {
	close(subscription.notifyCancelRequestChan)
	close(subscription.notifyRequestsChan)
	close(subscription.snapshotRequestsChan)
	subscription.snapshotsSubscription.close()
	close(subscription.snapshotsChan)
	subscription.rpc.close()
	subscription.mqSubscription.close()
	close(subscription.updatesChan)
}

// CreateNotification creates a new channel to notify about securities updates
// until CloseNotification will call for returned ID. Must be called only from
func (subscription *SecuritiesSubscription) CreateNotification() (
	SecuritiesSubscriptionNotificationID, <-chan SecurityStateList) {

	id := atomic.AddUint64(&subscription.prevNotificationID, 1)
	notifyChan := make(chan SecurityStateList, 1)
	subscription.notifyRequestsChan <- struct {
		id         SecuritiesSubscriptionNotificationID
		notifyChan chan SecurityStateList
	}{
		id:         id,
		notifyChan: notifyChan}
	return id, notifyChan
}

// CloseNotification cancels notification subscription and closes the channel.
func (subscription *SecuritiesSubscription) CloseNotification(
	id SecuritiesSubscriptionNotificationID) {

	subscription.notifyCancelRequestChan <- id
}

// Request calls passed callback in another goroutine and passes requested
// securities list as argument. Callback call is synced with notification, but
// use diffrent goroutines.
func (subscription *SecuritiesSubscription) Request(
	callback func(SecurityStateList)) {

	subscription.snapshotRequestsChan <- callback
}

func (subscription *SecuritiesSubscription) run() {
	defer func() {
		for _, notifyChan := range subscription.notifyChannels {
			close(notifyChan)
		}
	}()

	heartbeatTicker := time.NewTicker(1 * time.Minute)
	defer heartbeatTicker.Stop()
	for {
		select {
		case update, isOpen := <-subscription.updatesChan:
			if !isOpen {
				return
			}
			subscription.handleSecuritiesUpdate(
				update.exchange, update.update, update.source)
		case snapshot, isOpen := <-subscription.snapshotsChan:
			if !isOpen {
				return
			}
			subscription.handleSecuritiesSnapshot(
				snapshot.snapshot, snapshot.source)
		case callback, isOpen := <-subscription.snapshotRequestsChan:
			if !isOpen {
				return
			}
			subscription.handleSnapshotRequest(callback)
		case request, isOpen := <-subscription.notifyRequestsChan:
			if !isOpen {
				return
			}
			subscription.handleNotifyRequest(request)
		case id, isOpen := <-subscription.notifyCancelRequestChan:
			if !isOpen {
				return
			}
			subscription.handleNotifyCancelRequest(id)
		case <-heartbeatTicker.C:
			subscription.checkSources()
		}
	}
}

type securitiesUpdateMerger struct {
	subscription *SecuritiesSubscription
	changed      []SecurityState
	actionName   string
}

func (merger *securitiesUpdateMerger) merge(
	exchange string,
	updates securityStateListMessage,
	source string,
	hasPriority bool) {

	var securities *map[string]*struct {
		state   SecurityState
		sources map[string]struct{}
	}

	new := 0
	activated := 0
	deactivated := 0
	removed := 0

	for _, update := range updates {

		symbol, err := tradinglib.ImportSymbol(update.Type, update.Symbol)
		if err != nil {
			merger.subscription.exchange.trekt.LogErrorf(
				`Failed to import security symbol: "%s".`, err)
			continue
		}

		if securities == nil {
			var hasExchange bool
			securities, hasExchange = merger.subscription.securities[exchange]
			if !hasExchange {
				newNode := make(map[string]*struct {
					state   SecurityState
					sources map[string]struct{}
				})
				securities = &newNode
				merger.subscription.securities[exchange] = securities
			}
		}

		sourceRefs, hasSource := merger.subscription.sources[source]
		if !hasSource {
			newNode := make(map[string]*struct {
				state   SecurityState
				sources map[string]struct{}
			})
			sourceRefs = &newNode
			merger.subscription.sources[source] = sourceRefs
		}

		security, hasSecurity := (*securities)[update.ID]
		if hasSecurity {
			security.sources[source] = struct{}{}
			if !hasPriority {
				continue
			}
		}

		new++
		if update.IsActive != nil {
			if *update.IsActive {
				activated++
			} else {
				deactivated++
			}
			if hasSecurity &&
				security.state.IsActive != nil &&
				*update.IsActive == *security.state.IsActive {
				continue
			}
		} else {
			removed++
			if !hasSecurity {
				continue
			}
		}

		state := SecurityState{
			Security: tradinglib.Security{
				Symbol:   symbol,
				ID:       update.ID,
				Exchange: exchange},
			IsActive: update.IsActive}
		stateWithSources := &struct {
			state   SecurityState
			sources map[string]struct{}
		}{
			state:   state,
			sources: map[string]struct{}{source: struct{}{}}}

		(*securities)[update.ID] = stateWithSources
		(*sourceRefs)[update.ID] = stateWithSources
		merger.changed = append(merger.changed, state)
	}

	if securities == nil {
		return
	}

	merger.subscription.exchange.trekt.LogInfof(
		`Received %d securities in %s from "%s". Full list: %d`+
			", added: %d, removed: %d, activated: %d, deactivated: %d.",
		len(updates), merger.actionName, exchange, len(*securities),
		new, removed, activated, deactivated)
}

func (merger *securitiesUpdateMerger) notify() {
	merger.subscription.notify(merger.changed)
}

func (subscription *SecuritiesSubscription) handleSecuritiesUpdate(
	exchange string,
	update securityStateListMessage,
	source string) {

	merger := securitiesUpdateMerger{
		subscription: subscription,
		changed:      []SecurityState{},
		actionName:   "update"}
	merger.merge(exchange, update, source, true)
	merger.notify()
}

func (subscription *SecuritiesSubscription) handleSecuritiesSnapshot(
	snapshot map[string]securityStateListMessage,
	source string) {

	merger := securitiesUpdateMerger{
		subscription: subscription,
		changed:      []SecurityState{},
		actionName:   "snapshot"}
	for exchange, update := range snapshot {
		merger.merge(exchange, update, source, false)
	}
	merger.notify()
}

func (subscription *SecuritiesSubscription) checkSources() {
	for source := range subscription.sources {
		subscription.rpc.request(
			source,
			true,            // mandatory
			nil,             // request
			func([]byte) {}, // success handler
			func(err error) {
				securities, has := subscription.sources[source]
				if !has {
					return
				}
				changed := []SecurityState{}
				exchanges := map[string]struct{}{}
				delete(subscription.sources, source)
				for _, security := range *securities {
					delete(security.sources, source)
					if len(security.sources) == 0 {
						delete(*subscription.securities[security.state.Security.Exchange],
							security.state.Security.ID)
						security.state.IsActive = nil
						changed = append(changed, security.state)
						exchanges[security.state.Security.Exchange] = struct{}{}
					}
				}
				exchangesList := []string{}
				for exchange := range exchanges {
					exchangesList = append(exchangesList, exchange)
				}
				subscription.exchange.trekt.LogInfof(
					`Deleted %d securities from "%s"`+
						` by source "%s" heartbeat error "%s".`,
					len(changed), strings.Join(exchangesList, `", "`), source, err)
				subscription.notify(changed)
			}) // fail handler
	}
}

func (subscription *SecuritiesSubscription) notify(updates SecurityStateList) {
	for _, notifyChan := range subscription.notifyChannels {
		notifyChan <- updates
	}
}

func (subscription *SecuritiesSubscription) handleSnapshotRequest(
	callback func(SecurityStateList)) {

	list := SecurityStateList{}
	for _, exchange := range subscription.securities {
		for _, security := range *exchange {
			list = append(list, security.state)
		}
	}
	callback(list)
}

func (subscription *SecuritiesSubscription) handleNotifyRequest(
	request struct {
		id         SecuritiesSubscriptionNotificationID
		notifyChan chan SecurityStateList
	}) {

	subscription.notifyChannels[request.id] = request.notifyChan
}

func (subscription *SecuritiesSubscription) handleNotifyCancelRequest(
	id SecuritiesSubscriptionNotificationID) {
	if notifyCannel, has := subscription.notifyChannels[id]; has {
		close(notifyCannel)
		delete(subscription.notifyChannels, id)
	}
}

///////////////////////////////////////////////////////////////////////////////
