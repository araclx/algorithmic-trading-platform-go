// Copyright 2019 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rektra-network/trekt-go/pkg/mqclient"
	"github.com/rektra-network/trekt-go/pkg/tradinglib"
)

///////////////////////////////////////////////////////////////////////////////

type message struct{ topics []string }

func (message *message) export() string {
	return "[" + strings.Join(message.topics, ",") + "]"
}

func createEmptyMessage() message {
	return message{topics: []string{}}
}

func createMessage(topic, data string) message {
	return message{topics: []string{createMessageTopic(topic, data)}}
}

func (message *message) append(topic, data string) {
	message.topics = append(message.topics, createMessageTopic(topic, data))
}

func createMessageTopic(topic, data string) string {
	return `{"` + topic + `":` + data + `}`
}

///////////////////////////////////////////////////////////////////////////////

type protocolCache struct {
	exchanges map[string]struct{}
}

type protocol struct {
	cache protocolCache
}

func createProtocol() protocol {
	return protocol{cache: protocolCache{exchanges: make(map[string]struct{})}}
}
func (*protocol) close() {}

func (*protocol) parse(source []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal(source, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (*protocol) error(errorMessage string) message {
	return createMessage("error", errorMessage)
}

func (*protocol) authTopic() string { return "auth" }
func (protocol *protocol) authSuccess() message {
	return createMessage(protocol.authTopic(), "true")
}
func (protocol *protocol) authFail() message {
	return createMessage(protocol.authTopic(), "false")
}

func (*protocol) securityListTopic() string {
	return "securities"
}

func (protocol *protocol) securityList(
	list mqclient.SecurityStateList) message {

	exchanges := []string{}
	securities := map[string]*[]string{}
	for _, security := range list {

		securityType := ""
		var exporter func(tradinglib.Symbol) string
		switch security.Security.Symbol.GetType() {
		case tradinglib.SymbolTypeCurrencyPair:
			securityType = "currencyPair"
			exporter = createCurrencyPairSpecificMessagePart
		}
		if exporter == nil {
			continue
		}

		if _, has := protocol.cache.exchanges[security.Security.Exchange]; !has {
			protocol.cache.exchanges[security.Security.Exchange] = struct{}{}
			name := security.Security.Exchange
			if security.Security.Exchange == "binance" {
				name = "Binance"
			}
			exchanges = append(exchanges, fmt.Sprintf(`"%s":{"name":"%s"}`,
				security.Security.Exchange, name))
		}

		exchangeSecurities, hasExchange := securities[security.Security.Exchange]
		if !hasExchange {
			newNode := []string{}
			exchangeSecurities = &newNode
			securities[security.Security.Exchange] = exchangeSecurities
		}

		if security.IsActive == nil {
			*exchangeSecurities = append(*exchangeSecurities,
				fmt.Sprintf(`"%s":null`, security.Security.ID))
			continue
		}

		var isActive string
		if *security.IsActive {
			isActive = "true"
		} else {
			isActive = "false"
		}

		*exchangeSecurities = append(*exchangeSecurities,
			fmt.Sprintf(`"%s":{"isActive":%s,"type":"%s","name":"%s",%s}`,
				security.Security.ID,                 // node name
				isActive,                             // is active
				securityType,                         // type
				security.Security.Symbol.GetSymbol(), // name
				exporter(security.Security.Symbol)))  // type specific
	}

	result := createEmptyMessage()
	if len(exchanges) != 0 {
		result.append("exchanges", "{"+strings.Join(exchanges, ",")+"}")
	}
	{
		data := []string{}
		for exchange, exchangeSecurities := range securities {
			data = append(data,
				`"`+exchange+`":{`+strings.Join(*exchangeSecurities, ",")+"}")
		}
		result.append(
			protocol.securityListTopic(),
			"{"+strings.Join(data, "},{")+"}")
	}
	return result
}

func createCurrencyPairSpecificMessagePart(
	symbol tradinglib.Symbol) string {

	pair := symbol.(*tradinglib.CurrencyPairSymbol)
	return `"base":"` + pair.GetBaseCurrency() +
		`","quote":"` + pair.GetQuoteCurrency() + `"`
}

///////////////////////////////////////////////////////////////////////////////
