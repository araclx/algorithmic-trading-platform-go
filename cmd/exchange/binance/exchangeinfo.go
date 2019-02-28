// Copyright 2018 REKTRA Network, All Rights Reserved.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/rektra-network/trekt-go/pkg/tradinglib"
	t "github.com/rektra-network/trekt-go/pkg/trekt"
)

func runExchangeInfoUpdating(
	updateInterval time.Duration,
	trekt t.Trekt,
	stopChan <-chan interface{}) {

	updater := exchangeInfoUpdater{
		trekt:       trekt,
		securities:  make(map[string]t.SecurityState),
		updatesChan: make(chan t.SecurityStateList, 1),
	}
	defer close(updater.updatesChan)

	go func() {
		exchange := trekt.CreateSecuritiesExchangeOrExit(1)
		server := exchange.CreateServerOrExit()
		server.RunOrExit(updater.updatesChan)
		server.Close()
		exchange.Close()
	}()

	for {

		isSuccess := false
		for i := 0; !isSuccess && i < 3; i++ {
			isSuccess = updater.update()
		}

		sleepTime := updateInterval
		if !isSuccess {
			sleepTime /= 4
		}
		ticker := time.NewTicker(sleepTime)
		defer ticker.Stop()
		select {
		case <-ticker.C:
			break
		case <-stopChan:
			return
		}
	}
}

type symbolInfo struct {
	BaseAsset  string
	QuoteAsset string
	Symbol     string
	Status     string
}

type exchangeInfoUpdater struct {
	trekt       t.Trekt
	securities  map[string]t.SecurityState
	updatesChan chan t.SecurityStateList
}

func (updater *exchangeInfoUpdater) update() bool {
	symbols, err := requestSymbolInfo()
	if err != nil {
		updater.trekt.LogErrorf("%s.", err)
		return false
	}

	updates := make(t.SecurityStateList, 0)

	new := 0
	activated := 0
	deactivated := 0
	known := make(map[string]interface{})
	for _, symbol := range symbols {
		known[symbol.Symbol] = nil
		isActive := symbol.Status == "TRADING"

		security, isExistent := updater.securities[symbol.Symbol]
		if !isExistent {
			new++
		} else if *security.IsActive == isActive {
			continue
		}

		if isActive {
			activated++
		} else {
			deactivated++
		}

		security = t.SecurityState{
			Security: tradinglib.Security{
				Symbol: tradinglib.CreateCurrencyPairSymbol(
					symbol.BaseAsset,
					symbol.QuoteAsset),
				Exchange: updater.trekt.GetTypeName(),
				ID:       strings.ToLower(symbol.Symbol)},
			IsActive: &isActive}

		updater.securities[symbol.Symbol] = security
		updates = append(updates, security)
	}

	removed := 0
	if len(updater.securities) != len(known) {
		securities := make(map[string]t.SecurityState)
		for symbol, security := range updater.securities {
			if _, isKnown := known[symbol]; isKnown {
				securities[symbol] = security
			} else {
				security.IsActive = nil
				updates = append(updates, security)
				removed++
			}
		}
		updater.securities = securities
	}

	if len(updates) != 0 {
		updater.trekt.LogDebugf("%d securities updated, added: %d, removed: %d."+
			" Full list: %d, activate: %d, inactive: %d.",
			len(updates), new, removed,
			len(updater.securities), activated, deactivated)
		updater.updatesChan <- updates
	}

	return true

}

func requestSymbolInfo() ([]symbolInfo, error) {
	response, err := http.Get("https://api.binance.com//api/v1/exchangeInfo")
	if err != nil {
		return nil, fmt.Errorf(`Failed to request exchange info: "%s"`, err)
	}
	defer response.Body.Close()

	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf(
			`Failed to read exchange info request response: "%s"`, err)
	}

	var exchangeInfo map[string]interface{}
	err = json.Unmarshal(body, &exchangeInfo)
	if err != nil {
		return nil,
			fmt.Errorf(`Failed to parse exchange info request response: "%s"`, err)
	}

	symbolsNode, isFormatOk := exchangeInfo["symbols"]
	if !isFormatOk {
		return nil, fmt.Errorf(
			`Failed to parse exchange info request response (bad format): "%s"`, err)
	}

	var result []symbolInfo
	err = mapstructure.Decode(symbolsNode, &result)
	if err != nil {
		return nil, fmt.Errorf(
			`Failed to parse exchange info request response (bad symbol format):`+
				` "%s"`,
			err)
	}

	return result, nil
}
