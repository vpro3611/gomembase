package wal

import (
	"bufio"
	"errors"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"os"
)

type WalInterface interface {
	WriteToWal(actionString string) (int, error)
	RecoverFromWal(applyFunc func(line string) error) error
	CloseWal() error
	SyncWal() error
	TruncateWal() error
	WriteRaw(actionString string) (int, error)
}

type Wal struct {
	path string
	file *os.File
}

func NewWal(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, pkgerrors.WalError{Path: path, Err: errors.Join(pkgerrors.WalFailedToCreateError, err), Operation: "CREATE"}
	}
	return &Wal{path: path, file: file}, nil
}

func (w *Wal) Path() string {
	return w.path
}

func (w *Wal) File() *os.File {
	return w.file
}

func (w *Wal) WriteToWal(actionString string) (n int, err error) {
	bytes := []byte(actionString)
	n, err = w.file.Write(bytes)
	if err != nil {
		err = pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToWriteError, err, w.CloseWal()), Operation: "WRITE"}
		return 0, err
	}
	if syncErr := w.SyncWal(); syncErr != nil {
		return n, syncErr
	}
	return n, nil
}

func (w *Wal) WriteRaw(actionString string) (n int, err error) {
	bytes := []byte(actionString)
	n, err = w.file.Write(bytes)
	if err != nil {
		err = pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToWriteError, err, w.CloseWal()), Operation: "WRITE"}
		return 0, err
	}
	return n, nil
}

func (w *Wal) RecoverFromWal(applyFunc func(line string) error) error {
	// Reset file pointer to beginning to read all entries.
	// Note: O_APPEND only forces Write calls to the end, it doesn't affect Read/Seek.
	if _, err := w.file.Seek(0, 0); err != nil {
		return pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToReadError, err), Operation: "READ"}
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		if err := applyFunc(scanner.Text()); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToReadError, err), Operation: "READ"}
	}

	// Move pointer back to end as a courtesy, though O_APPEND ensures writes always append.
	if _, err := w.file.Seek(0, 2); err != nil {
		return pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToReadError, err), Operation: "READ"}
	}

	return nil
}

func (w *Wal) CloseWal() error {
	if err := w.file.Close(); err != nil {
		return pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToCloseError, err), Operation: "CLOSE"}
	}
	return nil
}

func (w *Wal) SyncWal() error {
	if err := w.file.Sync(); err != nil {
		return pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToSyncError, err), Operation: "SYNC"}
	}
	return nil
}

func (w *Wal) TruncateWal() error {
	if err := os.Truncate(w.path, 0); err != nil {
		return pkgerrors.WalError{Path: w.path, Err: errors.Join(pkgerrors.WalFailedToTruncateError, err), Operation: "TRUNCATE"}
	}
	return nil
}
