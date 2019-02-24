// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"

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

type securityUpdateMessage struct {
	Symbol    interface{}
	Type      string
	PricePrec uint16
	QtyPrec   uint16
	IsActive  *bool
}
type securityUpdateListMessage struct {
	SeqNum  uint64
	Updates map[string]securityUpdateMessage
}

///////////////////////////////////////////////////////////////////////////////

// SecuritiesExchange represents security states exchange.
type SecuritiesExchange struct{ mqChannel }

func createSecuritiesExchange(
	trekt Trekt, mq *Mq, capacity uint16) (*SecuritiesExchange, error) {

	result := &SecuritiesExchange{}
	err := result.mqChannel.init("securities", "topic", trekt, mq, capacity)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the exchange.
func (exchange *SecuritiesExchange) Close() { exchange.mqChannel.Close() }

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
	exchange       *SecuritiesExchange
	securities     map[string]map[string]securityUpdateMessage
	heartbeat      MqHeartbeatServer
	snapshotServer *mqSubscription
	sequenceNumber uint64
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

	server.securities = map[string]map[string]securityUpdateMessage{}
	defer server.unregisterAll()

	var err error
	server.heartbeat, err = CreateMqHeartbeatServer(&server.exchange.mqChannel,
		server.exchange.trekt)
	if err != nil {
		return err
	}
	defer server.heartbeat.Close()

	server.snapshotServer, err = createMqSubscription(
		"*.request", &server.exchange.mqChannel, true, server.exchange.trekt)
	if err != nil {
		return err
	}
	defer server.snapshotServer.close()
	requestChan := make(chan amqp.Delivery)
	defer close(requestChan)
	go server.snapshotServer.handle(func(request amqp.Delivery) {
		requestChan <- request
	})

	for {
		select {
		case update, isOpened := <-updatesChan:
			if !isOpened {
				return nil
			}
			server.broadcast(server.merge(update))
		case request, isOpen := <-requestChan:
			if !isOpen {
				return nil
			}
			server.handleSnapshotRequest(request)
		}
	}

}

func (server *SecuritiesServer) merge(
	update SecurityStateList) map[string]map[string]securityUpdateMessage {

	result := map[string]map[string]securityUpdateMessage{}

	for _, update := range update {
		symbol := update.Security.Symbol.Export()
		securityType := update.Security.Symbol.GetType()

		{
			message := securityUpdateMessage{
				Symbol:    symbol,
				Type:      securityType,
				PricePrec: update.Security.PricePrecision,
				QtyPrec:   update.Security.QtyPrecision,
				IsActive:  update.IsActive}
			if list, has := result[update.Security.Exchange]; !has {
				result[update.Security.Exchange] =
					map[string]securityUpdateMessage{update.Security.ID: message}
			} else {
				list[update.Security.ID] = message
			}
		}

		exchange, hasExchange := server.securities[update.Security.Exchange]

		if update.IsActive == nil {
			if hasExchange {
				delete(exchange, update.Security.ID)
			}
			continue
		}

		snapshot := securityUpdateMessage{
			Symbol:    symbol,
			Type:      securityType,
			PricePrec: update.Security.PricePrecision,
			QtyPrec:   update.Security.QtyPrecision,
			IsActive:  update.IsActive}
		if hasExchange {
			exchange[update.Security.ID] = snapshot
		} else {
			server.securities[update.Security.Exchange] =
				map[string]securityUpdateMessage{update.Security.ID: snapshot}
		}

	}

	return result
}

func (server *SecuritiesServer) broadcast(
	source map[string]map[string]securityUpdateMessage) {

	for exchange, securities := range source {
		message := amqp.Publishing{
			ReplyTo: server.heartbeat.GetAddress(), ContentType: "application/json"}
		var err error
		message.Body, err = json.Marshal(&securityUpdateListMessage{
			SeqNum: server.sequenceNumber, Updates: securities})
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to serialize security state list: "%s".`, err)
			continue
		}
		err = server.exchange.Publish(exchange+".update", false, false, message)
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to publish security state list: "%s".`, err)
			continue
		}
		server.sequenceNumber++
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
	message := map[string]map[string]securityUpdateMessage{}
	for exchangeID, securities := range server.securities {
		exchange, hasExchange := message[exchangeID]
		if !hasExchange {
			exchange = map[string]securityUpdateMessage{}
			message[exchangeID] = exchange
		}
		for id := range securities {
			exchange[id] = securityUpdateMessage{IsActive: nil}
		}
	}
	server.broadcast(message)
	server.securities = map[string]map[string]securityUpdateMessage{}
}

func (server *SecuritiesServer) handleSnapshotRequest(
	requestMessage amqp.Delivery) {

	request :=
		requestMessage.RoutingKey[:strings.Index(requestMessage.RoutingKey, ".")]
	var response map[string]securityUpdateListMessage
	if request == "*" {
		for exchange, securities := range server.securities {
			response[exchange] = securityUpdateListMessage{
				SeqNum:  server.sequenceNumber,
				Updates: securities}
		}
	} else if snapshot, has := server.securities[request]; has {
		response[request] = securityUpdateListMessage{
			SeqNum:  server.sequenceNumber,
			Updates: snapshot}
	} else {
		response[request] = securityUpdateListMessage{
			SeqNum:  server.sequenceNumber,
			Updates: map[string]securityUpdateMessage{}}
	}

	responseMessage := amqp.Publishing{
		ReplyTo:     server.heartbeat.GetAddress(),
		ContentType: "application/json"}
	var err error
	responseMessage.Body, err = json.Marshal(response)
	if err != nil {
		server.exchange.trekt.LogErrorf(
			`Failed to serialize security state list: "%s".`, err)
		return
	}
	err = server.snapshotServer.mq.Publish(
		requestMessage.ReplyTo, // key
		false,                  // mandatory
		false,                  // immediate
		responseMessage)
	if err != nil {
		server.exchange.trekt.LogErrorf(
			`Failed to publish security state list: "%s".`, err)
	}
	server.sequenceNumber++

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

	updatesChan chan struct {
		update           securityUpdateListMessage
		exchange, source string
	}

	snapshotsChan chan struct {
		snapshot map[string]securityUpdateListMessage
		source   string
	}
	snapshotsSubscription *mqSubscription

	securities map[string]*struct {
		sequenceNumber uint64
		source         string
		states         map[string]*SecurityState
	}

	snapshotRequestsChan chan func(SecurityStateList)

	prevNotificationID SecuritiesSubscriptionNotificationID
	notifyRequestsChan chan struct {
		id         SecuritiesSubscriptionNotificationID
		notifyChan chan SecurityStateList
	}
	notifyCancelRequestChan chan SecuritiesSubscriptionNotificationID
	notifyChannels          map[SecuritiesSubscriptionNotificationID]chan SecurityStateList

	heartbeat MqHeartbeatClient
}

func createSecuritiesSubscription(
	exchange *SecuritiesExchange,
	clientsCapacity uint16) (*SecuritiesSubscription, error) {

	result := &SecuritiesSubscription{
		securities: map[string]*struct {
			sequenceNumber uint64
			source         string
			states         map[string]*SecurityState
		}{}}

	result.updatesChan = make(chan struct {
		update           securityUpdateListMessage
		exchange, source string
	}, 1)
	err := result.mqSubscription.init("*.update", &exchange.mqChannel, true,
		exchange.trekt)
	if err != nil {
		close(result.updatesChan)
		return nil, err
	}
	go result.handle(func(message amqp.Delivery) {
		update := struct {
			update           securityUpdateListMessage
			exchange, source string
		}{exchange: message.RoutingKey[:strings.Index(message.RoutingKey, ".")],
			source: message.ReplyTo}
		err := json.Unmarshal(message.Body, &update.update)
		if err != nil {
			result.mqSubscription.trekt.LogErrorf(
				`Failed to parse security list update: "%s".`, err)
		}
		result.updatesChan <- update
	})

	result.snapshotsChan = make(chan struct {
		snapshot map[string]securityUpdateListMessage
		source   string
	}, 1)
	result.snapshotsSubscription, err = createMqSubscription("", result.mq, true,
		exchange.trekt)
	if err != nil {
		close(result.snapshotsChan)
		result.mqSubscription.close()
		close(result.updatesChan)
		return nil, err
	}
	go result.snapshotsSubscription.handle(func(message amqp.Delivery) {
		snapshot := struct {
			snapshot map[string]securityUpdateListMessage
			source   string
		}{source: message.ReplyTo}
		err := json.Unmarshal(message.Body, &snapshot.snapshot)
		if err != nil {
			result.trekt.LogErrorf(`Failed to parse security list snapshot: "%s".`,
				err)
		}
		result.snapshotsChan <- snapshot
	})
	err = result.mq.Publish("*.request", false, false,
		amqp.Publishing{ReplyTo: result.snapshotsSubscription.queue.Name})
	if err != nil {
		result.snapshotsSubscription.close()
		close(result.snapshotsChan)
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
	}{id: id, notifyChan: notifyChan}
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

	var err error
	subscription.heartbeat, err = CreateMqHeartbeatClient(
		subscription.mq, subscription.mqSubscription.trekt)
	if err != nil {
		subscription.mqSubscription.trekt.LogErrorf(
			`Failed to initialize heartbeat client: "%s".`, err)
	} else {
		defer subscription.heartbeat.Close()
	}

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
		case fail := <-subscription.heartbeat.GetFailedTestsChan():
			subscription.removeDisconnectedSource(fail.Address, fail.Err)
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
	updates securityUpdateListMessage,
	source string,
	hasPriority bool,
	heartbeat MqHeartbeatClient) {

	securities := merger.subscription.securities[exchange]
	if securities == nil {
		if updates.SeqNum != 0 {
			return
		}
		securities = &struct {
			sequenceNumber uint64
			source         string
			states         map[string]*SecurityState
		}{sequenceNumber: updates.SeqNum,
			source: source,
			states: map[string]*SecurityState{}}
		merger.subscription.securities[exchange] = securities
		heartbeat.AddAddress(source)
	} else if updates.SeqNum == 0 {
		if source != securities.source {
			heartbeat.RemoveAddress(securities.source)
			securities.source = source
			heartbeat.AddAddress(securities.source)
		}
	} else if updates.SeqNum <= securities.sequenceNumber {
		return
	} else if source != securities.source {
		return
	}

	new := 0
	activated := 0
	deactivated := 0
	removed := 0

	if updates.SeqNum == 0 {
		for id, security := range securities.states {
			if _, has := updates.Updates[id]; !has {
				merger.changed = append(merger.changed, SecurityState{
					Security: security.Security, IsActive: nil})
				removed++
			}
		}
	}

	for id, update := range updates.Updates {
		security := securities.states[id]
		isNew := false
		if security == nil {
			if update.IsActive == nil {
				continue
			}
			security = &SecurityState{}
			securities.states[id] = security
			isNew = true
			new++
		}

		symbol, err := tradinglib.ImportSymbol(update.Type, update.Symbol)
		if err != nil {
			merger.subscription.trekt.LogErrorf(
				`Failed to import security symbol: "%s".`, err)
			continue
		}
		if update.IsActive != nil {
			security.Security = tradinglib.Security{
				Symbol:         symbol,
				ID:             id,
				Exchange:       exchange,
				PricePrecision: update.PricePrec,
				QtyPrecision:   update.QtyPrec}
			if !isNew && *update.IsActive != *security.IsActive {
				if *update.IsActive {
					activated++
				} else {
					deactivated++
				}
			}
		} else {
			removed++
			delete(securities.states, id)
		}
		security.IsActive = update.IsActive

		merger.changed = append(merger.changed, *security)
	}

	securities.sequenceNumber = updates.SeqNum

	merger.subscription.trekt.LogInfof(
		`Received %d securities in %s with sequence number %d from "%s" (%s).`+
			" Full list: %d, added: %d, removed: %d, activated: %d, deactivated: %d.",
		len(updates.Updates), merger.actionName, updates.SeqNum, exchange, source,
		len(securities.states), new, removed, activated, deactivated)
}

func (merger *securitiesUpdateMerger) notify() {
	merger.subscription.notify(merger.changed)
}

func (subscription *SecuritiesSubscription) handleSecuritiesUpdate(
	exchange string, update securityUpdateListMessage, source string) {

	merger := securitiesUpdateMerger{
		subscription: subscription,
		changed:      []SecurityState{},
		actionName:   "update"}
	merger.merge(exchange, update, source, true, subscription.heartbeat)
	merger.notify()
}

func (subscription *SecuritiesSubscription) handleSecuritiesSnapshot(
	snapshot map[string]securityUpdateListMessage, source string) {

	merger := securitiesUpdateMerger{
		subscription: subscription,
		changed:      []SecurityState{},
		actionName:   "snapshot"}
	for exchange, update := range snapshot {
		merger.merge(exchange, update, source, false, subscription.heartbeat)
	}
	merger.notify()
}

func (subscription *SecuritiesSubscription) removeDisconnectedSource(
	source string, heartbeatErr error) {

	changed := []SecurityState{}
	for exchange, securities := range subscription.securities {
		if securities.source != source {
			continue
		}
		delete(subscription.securities, exchange)
		for _, security := range securities.states {
			security.IsActive = nil
			changed = append(changed, *security)
		}
		subscription.trekt.LogInfof(`Deleted %d securities from "%s"`+
			` by source "%s" heartbeat error "%s".`,
			len(securities.states), exchange, securities.source, heartbeatErr)
	}
	subscription.notify(changed)
}

func (subscription *SecuritiesSubscription) notify(updates SecurityStateList) {
	for _, notifyChan := range subscription.notifyChannels {
		notifyChan <- updates
	}
}

func (subscription *SecuritiesSubscription) handleSnapshotRequest(
	callback func(SecurityStateList)) {

	list := SecurityStateList{}
	for _, securities := range subscription.securities {
		for _, security := range securities.states {
			list = append(list, *security)
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
