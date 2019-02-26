// Copyright 2019 REKTRA Network, All Rights Reserved.

package main_test

import (
	"testing"

	ap "github.com/rektra-network/trekt-go/cmd/accesspoint"
	tl "github.com/rektra-network/trekt-go/pkg/tradinglib"
	t "github.com/rektra-network/trekt-go/pkg/trekt"
)

func Test_Protocol_Dom(test *testing.T) {
	protocol := ap.CreateProtocol()
	{
		export := protocol.DepthOfMarket(t.DepthOfMarketUpdate{}).Export()
		if export != `[{"dom":[["",""]]}]` {
			test.Error("Wrong export: " + export)
		}
	}
	{
		update := t.DepthOfMarketUpdate{
			Security: tl.Security{
				ID:             "id",
				Exchange:       "exchange",
				PricePrecision: 8,
				QtyPrecision:   8},
			Levels: t.DepthOfMarketLevels{
				{1.12345678, 2.12345678},
				{2.12345678, 3.12345678},
				{4.12345678, 5.12345678}}}
		update.Levels[1].SetDeleted()
		export := protocol.DepthOfMarket(update).Export()
		control := `[{"dom":[["exchange","id"],[1.12345678,2.12345678]` +
			`,[2.12345678],[4.12345678,5.12345678]]}]`
		if export != control {
			test.Error("Wrong export: " + export)
		}
	}
	{
		update := t.DepthOfMarketUpdate{
			Security: tl.Security{
				ID:             "id",
				Exchange:       "exchange",
				PricePrecision: 2,
				QtyPrecision:   3},
			Levels: t.DepthOfMarketLevels{
				{1.12345678, 2.12365678},
				{2.12345678, 3.12345678},
				{4.12645678, 5.12365678}}}
		update.Levels[1].SetDeleted()
		export := protocol.DepthOfMarket(update).Export()
		control := `[{"dom":[["exchange","id"],[1.12,2.124],[2.12]` +
			`,[4.13,5.124]]}]`
		if export != control {
			test.Error("Wrong export: " + export)
		}
	}
	defer protocol.Close()
}
