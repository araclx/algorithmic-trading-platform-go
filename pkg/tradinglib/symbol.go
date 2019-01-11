// Copyright 2018 REKTRA Network, All Rights Reserved.

package tradinglib

import "fmt"

const SymbolTypeCurrencyPair = "currencyPair"

// Symbol describes symbol of a trading instrument.
type Symbol interface {
	GetSymbol() string
	GetType() string
	Export() interface{}
}

// CurrencyPairSymbol describes currency pair symbol.
type CurrencyPairSymbol struct {
	baseCurrency  string
	quoteCurrency string
}

// ImportSymbol takes JSON created by exported interface and tries to create
// a symbol object from it.
func ImportSymbol(symbolType string, exported interface{}) (Symbol, error) {
	if symbolType == SymbolTypeCurrencyPair {
		return importCurrencyPairSymbol(exported)
	}
	return nil, fmt.Errorf(`Unknown symbol type "%s"`, symbolType)
}

// CreateCurrencyPairSymbol creates currency pair symbol instance.
func CreateCurrencyPairSymbol(baseCurrency, quoteCurrency string) Symbol {
	return &CurrencyPairSymbol{
		baseCurrency:  baseCurrency,
		quoteCurrency: quoteCurrency}
}

func importCurrencyPairSymbol(exported interface{}) (Symbol, error) {
	list, isOk := exported.([]interface{})
	if !isOk || len(list) != 2 {
		return nil, fmt.Errorf(`Exported symbol has wrong format or length: "%s"`,
			exported)
	}
	result := &CurrencyPairSymbol{}
	if result.baseCurrency, isOk = list[0].(string); !isOk {
		return nil, fmt.Errorf(`Exported base symbol has wrong format: "%s"`,
			exported)
	}
	if result.quoteCurrency, isOk = list[1].(string); !isOk {
		return nil, fmt.Errorf(`Exported quote symbol has wrong format: "%s"`,
			exported)
	}
	return result, nil
}

// GetSymbol returns human readable symbol string.
func (symbol *CurrencyPairSymbol) GetSymbol() string {
	return symbol.baseCurrency + "/" + symbol.quoteCurrency
}

// GetType returns symbol type.
func (symbol *CurrencyPairSymbol) GetType() string {
	return SymbolTypeCurrencyPair
}

// Export exports symbol as an abstract interface to serialize it.
func (symbol *CurrencyPairSymbol) Export() interface{} {
	return []string{symbol.baseCurrency, symbol.quoteCurrency}
}

// GetBaseCurrency returns base currency code.
func (symbol *CurrencyPairSymbol) GetBaseCurrency() string {
	return symbol.baseCurrency
}

// GetQuoteCurrency returns quote currency code.
func (symbol *CurrencyPairSymbol) GetQuoteCurrency() string {
	return symbol.quoteCurrency
}
