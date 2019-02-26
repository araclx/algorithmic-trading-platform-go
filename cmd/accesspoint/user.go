// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

type user struct {
	auth    trekt.Auth
	methods map[string]func(topic string, data interface{}) bool

	depthOfMarketSubscriptions []*trekt.MarketDataSubscription
}

func createUser(auth trekt.Auth) user {
	result := user{auth: auth}
	result.methods = map[string]func(string, interface{}) bool{}
	result.depthOfMarketSubscriptions = []*trekt.MarketDataSubscription{}
	return result
}

func (user *user) close() {
	for _, subscription := range user.depthOfMarketSubscriptions {
		subscription.Close()
	}
}
