// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
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

	exchangeInfoUpdatingStopChan := make(chan interface{})
	defer close(exchangeInfoUpdatingStopChan)
	exchangeInfoUpdatingStopWaiting := sync.WaitGroup{}
	defer exchangeInfoUpdatingStopWaiting.Wait()
	go func() {
		exchangeInfoUpdatingStopWaiting.Add(1)
		defer exchangeInfoUpdatingStopWaiting.Done()
		runExchangeInfoUpdating(
			15*time.Minute, trekt, exchangeInfoUpdatingStopChan)
	}()

	streamClientStopChan := make(chan interface{})
	defer close(streamClientStopChan)
	go func() {
		interruptChan := make(chan os.Signal, 1)
		defer close(interruptChan)
		signal.Notify(interruptChan, os.Interrupt)
		<-interruptChan
		streamClientStopChan <- nil
		exchangeInfoUpdatingStopChan <- nil
	}()

	runStreamClient(exchange, streamClientStopChan)

}
