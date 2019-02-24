// Copyright 2019 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rektra-network/trekt-go/pkg/tradinglib"
	"github.com/rektra-network/trekt-go/pkg/trekt"
)

///////////////////////////////////////////////////////////////////////////////

// Message represents serialized message.
type Message struct{ topics []string }

// Export returns serializes string.
func (message Message) Export() string {
	return "[" + strings.Join(message.topics, ",") + "]"
}

func createEmptyMessage() Message {
	return Message{topics: []string{}}
}

func createMessage(topic, data string) Message {
	return Message{topics: []string{createMessageTopic(topic, data)}}
}

func (message *Message) append(topic, data string) {
	message.topics = append(message.topics, createMessageTopic(topic, data))
}

func createMessageTopic(topic, data string) string {
	return `{"` + topic + `":` + data + `}`
}

///////////////////////////////////////////////////////////////////////////////

type protocolCache struct {
	exchanges map[string]interface{}
}

// Protocol encapsulates messages serialization.
type Protocol struct {
	cache protocolCache
}

// CreateProtocol creates protocol object.
func CreateProtocol() Protocol {
	return Protocol{cache: protocolCache{
		exchanges: make(map[string]interface{})}}
}

// Close cleanups protocol object.
func (Protocol) Close() {}

func (Protocol) parse(source []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal(source, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (Protocol) error(errorMessage string) Message {
	return createMessage("error", errorMessage)
}

func (Protocol) authTopic() string { return "auth" }
func (protocol Protocol) authSuccess() Message {
	return createMessage(protocol.authTopic(), "true")
}
func (protocol Protocol) authFail() Message {
	return createMessage(protocol.authTopic(), "false")
}

func (Protocol) securityListTopic() string { return "securities" }

func (protocol Protocol) securityList(
	list trekt.SecurityStateList) Message {

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
			protocol.cache.exchanges[security.Security.Exchange] = nil
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

func (Protocol) depthOfMarketTopic() string { return "dom" }

// DepthOfMarket serializes depth of market update.
func (protocol Protocol) DepthOfMarket(
	source trekt.DepthOfMarketUpdate) Message {

	result := `[["` + source.Security.Exchange + `","` + source.Security.ID + `"]`
	for _, level := range source.Levels {
		if !level.IsDeleted() {
			format := fmt.Sprintf(",[%%.%df,%%.%df]",
				source.Security.PricePrecision,
				source.Security.QtyPrecision)
			result += fmt.Sprintf(format, level.GetPrice(), level.GetQty())
		} else {
			format := fmt.Sprintf(",[%%.%df]", source.Security.PricePrecision)
			result += fmt.Sprintf(format, level.GetPrice())
		}
	}
	result += "]"
	return createMessage(protocol.depthOfMarketTopic(), result)
}

///////////////////////////////////////////////////////////////////////////////
