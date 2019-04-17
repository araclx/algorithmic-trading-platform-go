// Copyright 2019 REKTRA Network, All Rights Reserved.

package main

import (
	"sync/atomic"

	"github.com/mitchellh/mapstructure"
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

func (connection *connection) authorize(topic string, data interface{}) bool {

	delete(connection.user.methods, topic)
	request := trekt.AuthRequest{}
	err := mapstructure.Decode(data, &request)
	if err != nil {
		connection.logWarnf(
			`Received authorize-request in the wrong format "%s": "%s".`,
			data, err)
		return false
	}

	connection.logDebugf(`Authorizing with login "%s"...`, request.Login)
	connection.service.auth.Request(
		request,
		func(auth trekt.Auth) {
			connection.strandChan <- func() bool {
				if !connection.initUser(auth) {
					connection.send(connection.protocol.error("Internal error."))
					return false
				}
				atomic.AddInt32(&connection.isLogged, 1)
				connection.logInfof(`Successfully authorized with login "%s".`,
					request.Login)
				connection.send(connection.protocol.authSuccess())
				return true
			}
		},
		func(err error) {
			connection.logInfof(`Failed to authorize with login "%s": "%s".`,
				request.Login, err)
			connection.send(connection.protocol.authFail())
			connection.strandChan <- func() bool { return false }
		})

	return true
}

func (connection *connection) sendSecurityList(
	topic string, data interface{}) bool {

	connection.service.securities.Request(
		func(securities trekt.SecurityStateList) {
			connection.strandChan <- func() bool {
				if connection.securitiesSubscription.updatesChan == nil {
					connection.securitiesSubscription.id,
						connection.securitiesSubscription.updatesChan =
						connection.service.securities.CreateNotification()
				}
				connection.updateSecurities(securities)
				return true
			}
		})
	return true
}

func (connection *connection) startDepthOfMarket(
	topic string, data interface{}) bool {

	request := map[string][]string{}
	err := mapstructure.Decode(data, &request)
	if err != nil {
		connection.logWarnf(`DOM-request has invalid format "%s": "%s".`,
			data, err)
		return false
	}

	newSubscriptions := []*trekt.MarketDataSubscription{}
	for exchange, securities := range request {
		for _, securityID := range securities {
			security := connection.resolveSecurity(exchange, securityID)
			if security == nil {
				connection.logWarnf(
					"Requested depth of market for unknown or forbitten security"+
						` "%s" on "%s".`,
					securityID, exchange)
			}
			subscription, err := connection.service.marketData.StartDepthOfMarket(
				*security, connection.depthOfMarketUpdatesChan)
			connection.logDebugf(`Requesting depth of market for "%s" on "%s"...`,
				security.ID, security.Exchange)
			if err != nil {
				connection.logErrorf(
					`Failed to start depth of market for "%s" on "%s": "%s".`,
					security, exchange, err)
				continue
			}
			newSubscriptions = append(newSubscriptions, subscription)
		}
	}

	for _, subscription := range connection.user.depthOfMarketSubscriptions {
		subscription.Close()
	}
	connection.user.depthOfMarketSubscriptions = newSubscriptions

	return true
}
