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

	RegexpError = errors.New("invalid regexp, compilation failed")
)

const (
	CodeKeyNotFound   = "KEY_NOT_FOUND"
	CodeKeyExpired    = "KEY_EXPIRED"
	CodeKeyNotInteger = "KEY_NOT_INTEGER"
	CodeInvalidRegexp = "INVALID_REGEXP"
	CodeWalError      = "WAL_ERROR"
	CodeSnapshotError = "SNAPSHOT_ERROR"
)

// APIError is implemented by custom errors that provide codes and HTTP statuses.
type APIError interface {
	error
	ErrorCode() string
	HTTPStatus() int
}

type KeyError struct {
	Key       string
	Err       error
	Value     string // The invalid value or content if relevant (e.g. "abc")
	Operation string // The operation that failed (e.g. "GET", "INCREMENT")
}

func (e KeyError) Error() string {
	var details string
	if e.Operation != "" {
		details += fmt.Sprintf("during %s: ", e.Operation)
	}
	if e.Value != "" {
		details += fmt.Sprintf("invalid value %q: ", e.Value)
	}
	return fmt.Sprintf("key error: key %q: %s%v", e.Key, details, e.Err)
}

func (e KeyError) Unwrap() error {
	return e.Err
}

func (e KeyError) ErrorCode() string {
	switch {
	case errors.Is(e.Err, KeyNotFoundError):
		return CodeKeyNotFound
	case errors.Is(e.Err, KeyExpiredError):
		return CodeKeyExpired
	case errors.Is(e.Err, KeyNotIntegerError):
		return CodeKeyNotInteger
	default:
		return "KEY_ERROR"
	}
}

func (e KeyError) HTTPStatus() int {
	switch {
	case errors.Is(e.Err, KeyNotFoundError):
		return 404
	case errors.Is(e.Err, KeyExpiredError):
		return 410
	case errors.Is(e.Err, KeyNotIntegerError):
		return 400
	default:
		return 500
	}
}

type WalError struct {
	Path      string
	Err       error
	Operation string // e.g. "CREATE", "WRITE", "READ", "SYNC"
}

func (e WalError) Error() string {
	var details string
	if e.Operation != "" {
		details += fmt.Sprintf("during %s: ", e.Operation)
	}
	return fmt.Sprintf("wal error at path %q: %s%v", e.Path, details, e.Err)
}

func (e WalError) Unwrap() error {
	return e.Err
}

func (e WalError) ErrorCode() string {
	return CodeWalError
}

func (e WalError) HTTPStatus() int {
	return 500
}

type SnapshotError struct {
	Path      string
	Err       error
	Operation string // e.g. "CREATE", "WRITE", "READ", "DELETE"
}

func (e SnapshotError) Error() string {
	var details string
	if e.Operation != "" {
		details += fmt.Sprintf("during %s: ", e.Operation)
	}
	if e.Path != "" {
		return fmt.Sprintf("snapshot error at path %q: %s%v", e.Path, details, e.Err)
	}
	return fmt.Sprintf("snapshot error: %s%v", details, e.Err)
}

func (e SnapshotError) Unwrap() error {
	return e.Err
}

func (e SnapshotError) ErrorCode() string {
	return CodeSnapshotError
}

func (e SnapshotError) HTTPStatus() int {
	return 500
}

type RegexError struct {
	RegexExpr string
	Err       error
}

func (e RegexError) Error() string {
	return fmt.Sprintf("regex error: invalid expression %q: %v", e.RegexExpr, e.Err)
}

func (e RegexError) Unwrap() error {
	return e.Err
}

func (e RegexError) ErrorCode() string {
	return CodeInvalidRegexp
}

func (e RegexError) HTTPStatus() int {
	return 400
}
