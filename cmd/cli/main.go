// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"flag"
	"log"
	"strings"

	"github.com/rektra-network/trekt-go/pkg/trekt"
)

var (
	mqBroker = flag.String("mq_broker",
		"localhost", "message queuing broker")
	streamBrokers = flag.String("stream_brokers",
		"localhost:9092", "stream brokers, as a comma-separated list")
	name            = flag.String("name", "", "node instance name")
	showLog         = flag.Bool("log", false, "show log network log records")
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

	app := app{trekt: trekt.DealOrExit("cli",
		*name, *mqBroker, strings.Split(*streamBrokers, ","), 1)}
	defer app.close()

	if *showLog {
		app.startLogListening()
	}
	if *accessPointHost != "" {
		app.startAccessPointReading(
			*accessPointHost, *accessPointPath, *isUnsecured)
	}

	log.Printf("To exit press CTRL+C")
	foreverChan := make(chan bool)
	<-foreverChan

}
