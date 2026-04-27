//go:build integration
// +build integration

package transport

import (
	"testing"

	iTesting "github.com/MarwanAlsoltany/amqp-broker/internal/testing"
)

var (
	benchSinkConnection Connection
	benchSinkError      error
)

func BenchmarkConnectionManagerAssign(b *testing.B) {
	url := iTesting.RabbitMQBenchmarkURL(b)

	newMgr := func(b *testing.B, size int) *ConnectionManager {
		b.Helper()
		mgr := NewConnectionManager(url, &ConnectionManagerOptions{Size: size})
		if err := mgr.Init(b.Context()); err != nil {
			b.Fatalf("init: %v", err)
		}
		b.Cleanup(func() { _ = mgr.Close() })
		return mgr
	}

	b.Run("Size1", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newMgr(b, 1)
		b.ResetTimer()
		for b.Loop() {
			benchSinkConnection, benchSinkError = mgr.Assign(ConnectionPurposePublish)
		}
	})

	b.Run("Size3", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newMgr(b, 3)
		b.ResetTimer()
		for b.Loop() {
			benchSinkConnection, benchSinkError = mgr.Assign(ConnectionPurposePublish)
		}
	})

	b.Run("Size8", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newMgr(b, 8)
		b.ResetTimer()
		for b.Loop() {
			benchSinkConnection, benchSinkError = mgr.Assign(ConnectionPurposePublish)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		mgr := newMgr(b, 4)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchSinkConnection, benchSinkError = mgr.Assign(ConnectionPurposePublish)
			}
		})
	})
}
