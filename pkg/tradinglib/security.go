// Copyright 2018 REKTRA Network, All Rights Reserved.

package tradinglib

// Security describes a trading instrument for concrete trading exchange.
type Security struct {
	Symbol   Symbol
	ID       string
	Exchange string
}
