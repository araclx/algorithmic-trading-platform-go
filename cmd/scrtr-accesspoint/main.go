// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"net"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rektra-network/trekt-go/pkg/mqclient"
	"golang.org/x/crypto/acme/autocert"
)

var (
	mqBroker    = flag.String("mq_broker", "localhost", "message queuing broker")
	name        = flag.String("name", "", "node instance name")
	host        = flag.String("host", "*:8443", "server host and port")
	isUnsecured = flag.Bool("unsecured", false,
		"do not use secure connections for client connection")
	endpoint = flag.String("endpoint", "/", "endpoint request path")
	capacity = flag.Uint("capacity", 250, "capacity")
)

func main() {
	flag.Parse()

	mq := mqclient.DealOrExit(*mqBroker, "accesspoint", *name, uint16(*capacity))
	defer mq.Close()

	authExchange := mq.CreateAuthExchangeOrExit(uint16(*capacity))
	defer authExchange.Close()
	authService := authExchange.CreateServiceOrExit()
	defer authService.Close()

	securitiesExchange := mq.CreateSecuritiesExchangeOrExit(uint16(*capacity))
	defer securitiesExchange.Close()
	securitiesSubscription := securitiesExchange.CreateSubscriptionOrExit(
		uint16(*capacity))
	defer securitiesSubscription.Close()

	marketData := mq.CreateMarketDataExchangeOrExit(uint16(*capacity))
	defer marketData.Close()

	service := service{
		mq:         mq,
		auth:       authService,
		securities: securitiesSubscription,
		marketData: marketData}

	router := mux.NewRouter()
	router.HandleFunc(*endpoint, service.handle)

	server := &http.Server{
		Handler: router,
	}

	mq.LogDebugf(`Opening server at "%s%s"...`, *host, *endpoint)
	var listener net.Listener
	if !*isUnsecured {
		listener = autocert.NewListener(*host)
	} else {
		var err error
		listener, err = net.Listen("tcp", *host)
		if err != nil {
			mq.LogErrorf(`Failed to start listener: "%s".`, err)
			return
		}
	}
	defer listener.Close()
	{
		secureType := "Secured"
		if *isUnsecured {
			secureType = "Unsecured"
		}
		mq.LogDebugf(`%s server opened at "%s".`,
			secureType,
			listener.Addr().String())
	}
	server.Serve(listener)
	mq.LogDebugf(`Server is stopped.`)
}
