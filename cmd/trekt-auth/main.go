// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"errors"
	"flag"
	"strings"

	"github.com/rektra-network/trekt-go/pkg/trekt"
)

var (
	mqBroker = flag.String("mq_broker",
		"localhost", "message queuing broker")
	streamBrokers = flag.String("stream_brokers",
		"localhost:9092", "stream brokers, as a comma-separated list")
	name = flag.String("name", "", "node instance name")
)

func auth(login, password string) (*trekt.Auth, error) {
	if login != "guest" || password != "guest" {
		return nil, errors.New("Wrong login or password")
	}
	return &trekt.Auth{Login: login}, nil
}

func main() {
	flag.Parse()

	trekt := trekt.DealOrExit("auth",
		*name, *mqBroker, strings.Split(*streamBrokers, ","), 1)
	defer trekt.Close()

	exchange := trekt.CreateAuthExchangeOrExit(1)
	defer exchange.Close()

	server := exchange.CreateServerOrExit()
	defer server.Close()

	server.Handle(auth)
}
