// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"log"

	"github.com/rektra-network/trekt-go/pkg/mqclient"
)

var (
	mqBroker        = flag.String("mq_broker", "localhost", "message queuing broker")
	name            = flag.String("name", "", "node instance name")
	logRequest      = flag.String("log", "", "log request")
	accessPointHost = flag.String(
		"host", "", "address of access point to connect and test it")
	accessPointPath = flag.String("endpoint", "/", "access point server path")
	isUnsecured     = flag.Bool("unsecured", false,
		"do not use secure connections for client connection")
	printMessages = flag.Bool("print_messages",
		false, "if set - each incoming message will be printed"+
			" to the standard logger")
)

func main() {
	flag.Parse()

	app := app{mq: mqclient.DealOrExit(*mqBroker, "cli", *name, 1)}
	defer app.close()

	if *logRequest != "" {
		app.startLogListening(*logRequest)
	}
	if *accessPointHost != "" {
		app.startAccessPointReading(
			*accessPointHost, *accessPointPath, *isUnsecured)
	}

	log.Printf("To exit press CTRL+C")
	foreverChan := make(chan bool)
	<-foreverChan

}
