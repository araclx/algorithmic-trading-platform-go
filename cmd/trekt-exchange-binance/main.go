// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/rektra-network/trekt-go/pkg/mqclient"
)

var (
	mqBroker = flag.String("mq_broker", "localhost", "message queuing broker")
	name     = flag.String("name", "", "node instance name")
)

func main() {
	flag.Parse()

	mq := mqclient.DealOrExit(*mqBroker, "binance", *name, 1)
	defer mq.Close()

	exchange := mq.CreateMarketDataExchangeOrExit(1)
	defer exchange.Close()

	exchangeInfoUpdatingStopChan := make(chan struct{})
	go runExchangeInfoUpdating(15*time.Minute, mq, exchangeInfoUpdatingStopChan)
	defer func() {
		exchangeInfoUpdatingStopChan <- struct{}{}
		close(exchangeInfoUpdatingStopChan)
	}()

	streamClientStopChan := make(chan struct{})
	go runStreamClient(exchange, mq, streamClientStopChan)
	defer func() {
		streamClientStopChan <- struct{}{}
		close(streamClientStopChan)
	}()

	interruptChan := make(chan os.Signal, 1)
	defer close(interruptChan)
	signal.Notify(interruptChan, os.Interrupt)
	<-interruptChan

}
