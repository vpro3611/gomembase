package errors

import (
	"errors"
	"fmt"
)

var (
	KeyNotFoundError   = errors.New("key was not found")
	KeyExpiredError    = errors.New("key has expired")
	KeyNotIntegerError = errors.New("key is not an integer")

	WalFailedToCreateError   = errors.New("wal failed to create")
	WalFailedToWriteError    = errors.New("wal failed to write")
	WalFailedToReadError     = errors.New("wal failed to read")
	WalFailedToRecover       = errors.New("wal failed to recover")
	WalFailedToCloseError    = errors.New("wal failed to close")
	WalFailedToSyncError     = errors.New("wal failed to sync")
	WalFailedToTruncateError = errors.New("wal failed to truncate")

	ErrInvalidSnapshotMagic   = errors.New("invalid snapshot magic number")
	ErrInvalidSnapshotVersion = errors.New("unsupported snapshot version")
	ErrSnapshotWriteFailed    = errors.New("failed to write snapshot")
	ErrSnapshotReadFailed     = errors.New("failed to read snapshot")
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

type SnapshotError struct {
	Path string
	Err  error
}

func (e SnapshotError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("snapshot error (%s): %v", e.Path, e.Err)
	}
	return fmt.Sprintf("snapshot error: %v", e.Err)
}

func (e SnapshotError) Unwrap() error {
	return e.Err
}
