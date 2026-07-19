package main

import (
	"github.com/vpro3611/gomembase.git/pkg/list_storage"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/set_storage"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"github.com/vpro3611/gomembase.git/pkg/wal"
	"github.com/vpro3611/gomembase.git/pkg/zset_storage"
	"time"
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

	s := storage.NewStorage(pm)
	pm.RegisterEngine(s)

	listEng := list_storage.NewListStorage(pm)
	pm.RegisterEngine(listEng)

	setEng := set_storage.NewSetStorage(pm)
	pm.RegisterEngine(setEng)

	zsetEng := zset_storage.NewZSetStorage(pm)
	pm.RegisterEngine(zsetEng)

	// Restore state (snapshot + WAL)
	if err := pm.Restore(nil); err != nil {
		panic(err)
	}

	// Expiration cleanup
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.CleanupExpired()
			listEng.CleanupExpired()
			setEng.CleanupExpired()
			zsetEng.CleanupExpired()
		}
	}()

	// Periodic snapshotting
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := pm.SaveSnapshot(); err != nil {
				// Log error instead of panicking in goroutine
				println("Snapshot save failed:", err.Error())
			}
		}
	}()
}
