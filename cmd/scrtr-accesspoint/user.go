// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

type user struct {
	auth       trekt.Auth
	methods    map[string]func(topic string, data interface{}) bool
	marketData *trekt.MarketDataService
}

func createUser(auth trekt.Auth) user {
	result := user{auth: auth}
	result.methods = make(map[string]func(string, interface{}) bool)
	return result
}

func (user *user) close() {
	if user.marketData != nil {
		user.marketData.Close()
		user.marketData = nil
	}
}
