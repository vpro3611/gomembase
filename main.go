package main

import (
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"time"
)

func main() {
	w, walErr := wal.NewWal("test.wal")
	if walErr != nil {
		panic(walErr)
	}

	defer func(w *wal.Wal) {
		err := w.CloseWal()
		if err != nil {
			panic(err)
		}
	}(w)

	s := storage.NewStorage(w)
	snap := snapshot.NewSnapshot("test.rdb")

	// 1. Load from snapshot first
	if err := s.LoadFromSnapshot(&snap); err != nil {
		panic(err)
	}

	// 2. Load from WAL later than snapshot
	if err := s.Load(); err != nil {
		panic(err)
	}

	// Expiration cleanup
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.CleanupExpired()
		}
	}()

	// Periodic snapshotting
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := s.SaveSnapshot(&snap); err != nil {
				// Log error instead of panicking in goroutine
				println("Snapshot save failed:", err.Error())
			}
		}
	}()
}
