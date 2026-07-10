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

	loadErr := storage.Load()
	if loadErr != nil {
		panic(loadErr)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			storage.CleanupExpired()
		}
	}()

}
