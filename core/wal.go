package core

import (
	"bufio"
	"errors"
	"os"
)

type WalInterface interface {
	WriteToWal(actionString string) (int, error)
	RecoverFromWal(applyFunc func(line string) error) error
	CloseWal() error
	SyncWal() error
	TruncateWal() error
}

type Wal struct {
	Path string
	File *os.File
}

func NewWal(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, WalError{Path: path, Err: errors.Join(WalFailedToCreateError, err)}
	}
	return &Wal{Path: path, File: file}, nil
}

func (w *Wal) WriteToWal(actionString string) (n int, err error) {
	bytes := []byte(actionString)
	n, err = w.File.Write(bytes)
	if err != nil {
		err = WalError{Path: w.Path, Err: errors.Join(WalFailedToWriteError, err, w.CloseWal())}
		return 0, err
	}
	if syncErr := w.SyncWal(); syncErr != nil {
		return n, syncErr
	}
	return n, nil
}

func (w *Wal) RecoverFromWal(applyFunc func(line string) error) error {
	// Reset file pointer to beginning to read all entries.
	// Note: O_APPEND only forces Write calls to the end, it doesn't affect Read/Seek.
	if _, err := w.File.Seek(0, 0); err != nil {
		return WalError{Path: w.Path, Err: errors.Join(WalFailedToReadError, err)}
	}

	scanner := bufio.NewScanner(w.File)
	for scanner.Scan() {
		if err := applyFunc(scanner.Text()); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return WalError{Path: w.Path, Err: errors.Join(WalFailedToReadError, err)}
	}

	// Move pointer back to end as a courtesy, though O_APPEND ensures writes always append.
	if _, err := w.File.Seek(0, 2); err != nil {
		return WalError{Path: w.Path, Err: errors.Join(WalFailedToReadError, err)}
	}

	return nil
}

func (w *Wal) CloseWal() error {
	if err := w.File.Close(); err != nil {
		return WalError{Path: w.Path, Err: errors.Join(WalFailedToCloseError, err)}
	}
	return nil
}

func (w *Wal) SyncWal() error {
	if err := w.File.Sync(); err != nil {
		return WalError{Path: w.Path, Err: errors.Join(WalFailedToSyncError, err)}
	}
	return nil
}

func (w *Wal) TruncateWal() error {
	if err := os.Truncate(w.Path, 0); err != nil {
		return WalError{Path: w.Path, Err: errors.Join(WalFailedToTruncateError, err)}
	}
	return nil
}
