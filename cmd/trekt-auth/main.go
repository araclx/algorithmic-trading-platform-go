// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"errors"
	"flag"

	"github.com/rektra-network/trekt-go/pkg/mqclient"
)

var (
	mqBroker = flag.String("mq_broker", "localhost", "message queuing broker")
	name     = flag.String("name", "", "node instance name")
)

func auth(login, password string) (*mqclient.Auth, error) {
	if login != "guest" || password != "guest" {
		return nil, errors.New("Wrong login or password")
	}
	return &mqclient.Auth{Login: login}, nil
}

func main() {
	flag.Parse()

	mq := mqclient.DealOrExit(*mqBroker, "auth", *name)
	defer mq.Close()

	exchange := mq.CreateAuthExchangeOrExit()
	defer exchange.Close()

	server := exchange.CreateServerOrExit()
	defer server.Close()

	server.Handle(auth)
}
