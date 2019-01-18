// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"net"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/rektra-network/trekt-go/pkg/trekt"
	"golang.org/x/crypto/acme/autocert"
)

var (
	mqBroker = flag.String("mq_broker",
		"localhost", "message queuing broker")
	streamBrokers = flag.String("stream_brokers",
		"localhost:9092", "stream brokers, as a comma-separated list")
	name        = flag.String("name", "", "node instance name")
	host        = flag.String("host", "*:8443", "server host and port")
	isUnsecured = flag.Bool("unsecured", false,
		"do not use secure connections for client connection")
	endpoint = flag.String("endpoint", "/", "endpoint request path")
	capacity = flag.Uint("capacity", 250, "capacity")
)

func main() {
	flag.Parse()

	trekt := trekt.DealOrExit("accesspoint",
		*name, *mqBroker, strings.Split(*streamBrokers, ","), uint16(*capacity))
	defer trekt.Close()

	authExchange := trekt.CreateAuthExchangeOrExit(uint16(*capacity))
	defer authExchange.Close()
	authService := authExchange.CreateServiceOrExit()
	defer authService.Close()

	securitiesExchange := trekt.CreateSecuritiesExchangeOrExit(uint16(*capacity))
	defer securitiesExchange.Close()
	securitiesSubscription := securitiesExchange.CreateSubscriptionOrExit(
		uint16(*capacity))
	defer securitiesSubscription.Close()

	marketData := trekt.CreateMarketDataExchangeOrExit(uint16(*capacity))
	defer marketData.Close()

	service := service{
		trekt:      trekt,
		auth:       authService,
		securities: securitiesSubscription,
		marketData: marketData}

	router := mux.NewRouter()
	router.HandleFunc(*endpoint, service.handle)

	server := &http.Server{
		Handler: router,
	}

	trekt.LogDebugf(`Opening server at "%s%s"...`, *host, *endpoint)
	var listener net.Listener
	if !*isUnsecured {
		listener = autocert.NewListener(*host)
	} else {
		var err error
		listener, err = net.Listen("tcp", *host)
		if err != nil {
			trekt.LogErrorf(`Failed to start listener: "%s".`, err)
			return
		}
	}
	defer listener.Close()
	{
		secureType := "Secured"
		if *isUnsecured {
			secureType = "Unsecured"
		}
		trekt.LogDebugf(`%s server opened at "%s".`,
			secureType,
			listener.Addr().String())
	}
	server.Serve(listener)
	trekt.LogDebugf(`Server is stopped.`)
}
