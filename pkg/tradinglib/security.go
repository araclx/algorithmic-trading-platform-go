// Copyright 2018 REKTRA Network, All Rights Reserved.

package tradinglib

// SecurityKey describes an instrument unique key.
type SecurityKey struct{ exchange, security string }

// Security describes a trading instrument for concrete trading exchange.
type Security struct {
	// Human-readable security symbol. Maybe not unique.
	Symbol Symbol
	// Unique for exchange security ID.
	ID string
	// Exchange code.
	Exchange string
	// Number of decimal places in price.
	PricePrecision uint16
	// Number of decimal places in quantity.
	QtyPrecision uint16
}

// GetKey returns security unique key.
func (security Security) GetKey() SecurityKey {
	return SecurityKey{exchange: security.Exchange, security: security.ID}
}
