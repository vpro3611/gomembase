package core

import (
	"errors"
	"fmt"
)

var (
	KeyNotFoundError = errors.New("key was not found")
	KeyExpiredError  = errors.New("key has expired")

	WalFailedToCreateError   = errors.New("wal failed to create")
	WalFailedToWriteError    = errors.New("wal failed to write")
	WalFailedToReadError     = errors.New("wal failed to read")
	WalFailedToRecover       = errors.New("wal failed to recover")
	WalFailedToCloseError    = errors.New("wal failed to close")
	WalFailedToSyncError     = errors.New("wal failed to sync")
	WalFailedToTruncateError = errors.New("wal failed to truncate")
)

type KeyError struct {
	Key string
	Err error
}

func (e KeyError) Error() string {
	return fmt.Sprintf("key %s: %v", e.Key, e.Err)
}

func (e KeyError) Unwrap() error {
	return e.Err
}

type WalError struct {
	Path string
	Err  error
}

func (e WalError) Error() string {
	return fmt.Sprintf("wal error (%s) : %v", e.Path, e.Err)
}

func (e WalError) Unwrap() error {
	return e.Err
}
