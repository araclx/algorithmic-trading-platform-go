// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

import (
	"log"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

func closeChannel(channel **amqp.Channel) {
	if *channel == nil {
		return
	}
	err := (*channel).Close()
	if err != nil {
		log.Printf(`Failed to close channel: "%s".`, err)
		return
	}
	*channel = nil
}

///////////////////////////////////////////////////////////////////////////////

func generateUniqueConsumerTag() string {
	// The function is copied from github.com/streadway/amqp/consumers.go
	return generateCommandNameBasedUniqueConsumerTag(os.Args[0])
}

var consumerSeq uint64

func generateCommandNameBasedUniqueConsumerTag(commandName string) string {
	// The function is copied from github.com/streadway/amqp/consumers.go

	tagPrefix := "ctag-"
	tagInfix := commandName
	tagSuffix := "-" + strconv.FormatUint(atomic.AddUint64(&consumerSeq, 1), 10)

	const consumerTagLengthMax = 0xFF // see writeShortstr
	if len(tagPrefix)+len(tagInfix)+len(tagSuffix) > consumerTagLengthMax {
		tagInfix = "streadway/amqp"
	}

	return tagPrefix + tagInfix + tagSuffix
}

///////////////////////////////////////////////////////////////////////////////
