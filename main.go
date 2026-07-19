package main

import (
	"log"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/server"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

func main() {
	w, walErr := wal.NewWal("test.wal")
	if walErr != nil {
		panic(walErr)
	}

	bufferedW := wal.NewBufferedWal(w)

	defer func(bw *wal.BufferedWal) {
		err := bw.CloseWal()
		if err != nil {
			panic(err)
		}
	}(bufferedW)

	snap := snapshot.NewSnapshot("test.rdb")

	pm := persistence.NewPersistenceManager(bufferedW, &snap)

	// Background WAL flushing every 1 second
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := bufferedW.SyncWal(); err != nil {
				println("WAL flush failed:", err.Error())
			}
		}
	}()

	// Instantiate Multiplexer with limit of 5 sub-instances
	mux := multiplexer.NewMultiplexer(pm, 5)
	pm.RegisterEngine(mux)
	pm.RegisterFallbackEngine(mux)

	// Restore state (snapshot + WAL)
	if err := pm.Restore(nil); err != nil {
		panic(err)
	}

	// Expiration cleanup loop across all active sub-instances
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			mux.CleanupAllExpired()
		}
	}()

	// Periodic snapshotting every 5 minutes
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := pm.SaveSnapshot(); err != nil {
				println("Snapshot save failed:", err.Error())
			}
		}
	}()

	// Start TCP Server on port 6380
	srv := server.NewServer(mux, ":6380")
	log.Printf("Starting GObase TCP Server on :6380")
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
