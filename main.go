package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/vpro3611/gomembase.git/pkg/multiplexer"
	"github.com/vpro3611/gomembase.git/pkg/persistence"
	"github.com/vpro3611/gomembase.git/pkg/pubsub"
	"github.com/vpro3611/gomembase.git/pkg/server"
	"github.com/vpro3611/gomembase.git/pkg/snapshot"
	"github.com/vpro3611/gomembase.git/pkg/wal"
)

type EnvStructRaw struct {
	WALPath                   string
	SnapshotPath              string
	WALFlushInterval          string
	SnapshotInterval          string
	ExpirationCleanupInterval string
	MaxSubInstances           string
}

type EnvStructParsed struct {
	WALPath                   string
	SnapshotPath              string
	WALFlushInterval          time.Duration
	SnapshotInterval          time.Duration
	ExpirationCleanupInterval time.Duration
	MaxSubInstances           int
}

func LoadFromEnv() error {
	_ = godotenv.Load()
	return nil
}

// AssertEnv MUST be called after LoadFromEnv()
func AssertEnv() (EnvStructRaw, []error) {
	var errors []error
	var env EnvStructRaw
	walPath := os.Getenv("WAL_PATH")
	if walPath == "" {
		errors = append(errors, fmt.Errorf("WAL_PATH is not set"))
	}
	env.WALPath = walPath

	snapPath := os.Getenv("SNAPSHOT_PATH")
	if snapPath == "" {
		errors = append(errors, fmt.Errorf("SNAPSHOT_PATH is not set"))
	}
	env.SnapshotPath = snapPath

	walFlushInterval := os.Getenv("WAL_FLUSH_INTERVAL")
	if walFlushInterval == "" {
		errors = append(errors, fmt.Errorf("WAL_FLUSH_INTERVAL is not set"))
	}
	env.WALFlushInterval = walFlushInterval

	snapInterval := os.Getenv("SNAPSHOT_INTERVAL")
	if snapInterval == "" {
		errors = append(errors, fmt.Errorf("SNAPSHOT_INTERVAL is not set"))
	}
	env.SnapshotInterval = snapInterval

	expirationCleanupInterval := os.Getenv("EXPIRATION_CLEANUP_INTERVAL")
	if expirationCleanupInterval == "" {
		errors = append(errors, fmt.Errorf("EXPIRATION_CLEANUP_INTERVAL is not set"))
	}
	env.ExpirationCleanupInterval = expirationCleanupInterval

	maxSubInstances := os.Getenv("MAX_SUB_INSTANCES")
	if maxSubInstances == "" {
		errors = append(errors, fmt.Errorf("MAX_SUB_INSTANCES is not set"))
	}
	env.MaxSubInstances = maxSubInstances

	return env, errors
}

func ParseRawEnvStruct(env EnvStructRaw) EnvStructParsed {
	var parsedEnv EnvStructParsed
	parsedEnv.WALPath = env.WALPath
	parsedEnv.SnapshotPath = env.SnapshotPath
	parsedEnv.WALFlushInterval, _ = time.ParseDuration(env.WALFlushInterval)

	parsedEnv.SnapshotInterval, _ = time.ParseDuration(env.SnapshotInterval)
	parsedEnv.ExpirationCleanupInterval, _ = time.ParseDuration(env.ExpirationCleanupInterval)
	parsedEnv.MaxSubInstances, _ = strconv.Atoi(env.MaxSubInstances)
	return parsedEnv
}

func IdentifyPort() (string, error) {
	var port string
	if len(os.Args) > 1 {
		port = os.Args[1]
	} else if envPort := os.Getenv("PORT"); envPort != "" {
		port = envPort
	} else {
		port = "6381"
	}
	formattedPort := fmt.Sprintf(":%s", port)
	return formattedPort, nil
}

func main() {

	envErr := LoadFromEnv()
	if envErr != nil {
		panic(envErr)
	}

	env, errors := AssertEnv()
	if len(errors) > 0 {
		for _, err := range errors {
			log.Println(err.Error())
		}
		log.Panic("Environment variables are not set correctly\n")
	}

	envParsed := ParseRawEnvStruct(env)

	port, portErr := IdentifyPort()
	if portErr != nil {
		panic(portErr)
	}

	w, walErr := wal.NewWal(envParsed.WALPath)
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

	snap := snapshot.NewSnapshot(envParsed.SnapshotPath)

	pm := persistence.NewPersistenceManager(bufferedW, &snap)

	// Background WAL flushing every N second(s)
	go func() {
		ticker := time.NewTicker(envParsed.WALFlushInterval)
		defer ticker.Stop()
		for range ticker.C {
			if err := bufferedW.SyncWal(); err != nil {
				println("WAL flush failed:", err.Error())
			}
		}
	}()

	// Instantiate Multiplexer with limit
	mux := multiplexer.NewMultiplexer(pm, envParsed.MaxSubInstances)
	pm.RegisterEngine(mux)
	pm.RegisterFallbackEngine(mux)

	// Restore state (snapshot + WAL)
	if err := pm.Restore(nil); err != nil {
		panic(err)
	}

	// Expiration cleanup loop across all active sub-instances
	go func() {
		ticker := time.NewTicker(envParsed.ExpirationCleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			mux.CleanupAllExpired()
		}
	}()

	// Periodic snapshotting every N minutes
	go func() {
		ticker := time.NewTicker(envParsed.SnapshotInterval)
		defer ticker.Stop()
		for range ticker.C {
			if err := pm.SaveSnapshot(); err != nil {
				println("Snapshot save failed:", err.Error())
			}
		}
	}()

	// Start TCP Server on port ${port}
	hub := pubsub.NewHub()
	srv := server.NewServer(mux, hub, port)
	log.Printf("Starting GObase TCP Server on %s\n", port)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
