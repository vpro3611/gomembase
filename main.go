package main

import (
	"github.com/vpro3611/gomembase.git/core"
	"time"
)

func main() {
	wal, walErr := core.NewWal("test.wal")
	if walErr != nil {
		panic(walErr)
	}

	defer func(wal *core.Wal) {
		err := wal.CloseWal()
		if err != nil {
			panic(err)
		}
	}(wal)

	storage := core.NewStorage(wal)
	snapshot := core.NewSnapshot("test.rdb")

	// 1. Load from snapshot first
	if err := storage.LoadFromSnapshot(snapshot); err != nil {
		panic(err)
	}

	// 2. Load from WAL later than snapshot
	if err := storage.Load(); err != nil {
		panic(err)
	}

	// Expiration cleanup
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			storage.CleanupExpired()
		}
	}()

	// Periodic snapshotting
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := storage.SaveSnapshot(snapshot); err != nil {
				// Log error instead of panicking in goroutine
				println("Snapshot save failed:", err.Error())
			}
		}
	}()
}
