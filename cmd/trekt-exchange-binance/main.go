// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/rektra-network/trekt-go/pkg/trekt"
)

var (
	mqBroker = flag.String("mq_broker",
		"localhost", "message queuing broker")
	streamBrokers = flag.String("stream_brokers",
		"localhost:9092", "stream brokers, as a comma-separated list")
	name = flag.String("name", "", "node instance name")
)

func main() {
	flag.Parse()

	trekt := trekt.DealOrExit("binance",
		*name, *mqBroker, strings.Split(*streamBrokers, ","), 1)
	defer trekt.Close()

	exchange := trekt.CreateMarketDataExchangeOrExit(1)
	defer exchange.Close()

	exchangeInfoUpdatingStopChan := make(chan struct{})
	go runExchangeInfoUpdating(
		15*time.Minute, trekt, exchangeInfoUpdatingStopChan)
	defer func() {
		exchangeInfoUpdatingStopChan <- struct{}{}
		close(exchangeInfoUpdatingStopChan)
	}()

	streamClientStopChan := make(chan struct{})
	go runStreamClient(exchange, trekt, streamClientStopChan)
	defer func() {
		streamClientStopChan <- struct{}{}
		close(streamClientStopChan)
	}()

	interruptChan := make(chan os.Signal, 1)
	defer close(interruptChan)
	signal.Notify(interruptChan, os.Interrupt)
	<-interruptChan

}
