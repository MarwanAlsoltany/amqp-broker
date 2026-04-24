package topology

import (
	"testing"
)

func BenchmarkNewRoutingKey(b *testing.B) {
	b.Run("NoPlaceholders", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchSinkRoutingKey = NewRoutingKey("orders.created", nil)
		}
	})

	b.Run("1Placeholder", func(b *testing.B) {
		b.ReportAllocs()
		args := map[string]string{"region": "us-east"}
		b.ResetTimer()
		for b.Loop() {
			benchSinkRoutingKey = NewRoutingKey("orders.{region}.created", args)
		}
	})

	b.Run("3Placeholders", func(b *testing.B) {
		b.ReportAllocs()
		args := map[string]string{
			"region": "eu-west",
			"tenant": "acme",
			"action": "created",
		}
		b.ResetTimer()
		for b.Loop() {
			benchSinkRoutingKey = NewRoutingKey("orders.{region}.{tenant}.{action}", args)
		}
	})

	b.Run("5Placeholders", func(b *testing.B) {
		b.ReportAllocs()
		args := map[string]string{
			"region":  "us-east",
			"tenant":  "acme",
			"action":  "created",
			"version": "v2",
			"env":     "prod",
		}
		b.ResetTimer()
		for b.Loop() {
			benchSinkRoutingKey = NewRoutingKey("orders.{region}.{tenant}.{action}.{version}.{env}", args)
		}
	})
}
