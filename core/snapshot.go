package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

var (
	FirstBytes = [4]byte{'R', 'C', 'D', 'B'}
	Version    = uint16(1)
)

type SnapshotHeader struct {
	FirstBytes [4]byte
	Version    uint16
	Count      uint64
}

func CreateSnapshotHeader(count uint64) SnapshotHeader {
	return SnapshotHeader{FirstBytes: FirstBytes, Version: Version, Count: count}
}

func (sh *SnapshotHeader) Validate() error {
	expected := CreateSnapshotHeader(sh.Count)
	if sh.FirstBytes != expected.FirstBytes {
		return ErrInvalidSnapshotMagic
	}
	if sh.Version != expected.Version {
		return ErrInvalidSnapshotVersion
	}
	return nil
}

type UintV interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func WriteHeader(w io.Writer, count uint64) error {
	header := CreateSnapshotHeader(count)

	if _, err := w.Write(header.FirstBytes[:]); err != nil {
		return errors.Join(ErrSnapshotWriteFailed, err)
	}

	if err := WriteUintValue(w, header.Version); err != nil {
		return errors.Join(ErrSnapshotWriteFailed, err)
	}

	if err := WriteUintValue(w, header.Count); err != nil {
		return errors.Join(ErrSnapshotWriteFailed, err)
	}
	return nil
}

func WriteUintValue[T UintV](w io.Writer, value T) error {
	return binary.Write(w, binary.LittleEndian, value)
}

func WriteInt64Value(w io.Writer, value int64) error {
	return binary.Write(w, binary.LittleEndian, value)
}

func WriteBytes(w io.Writer, value []byte) error {
	if err := WriteUintValue(w, uint32(len(value))); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

func ReadSnapshotHeader(r io.Reader) (SnapshotHeader, error) {
	var header SnapshotHeader
	if _, err := io.ReadFull(r, header.FirstBytes[:]); err != nil {
		return SnapshotHeader{}, errors.Join(ErrSnapshotReadFailed, err)
	}
	version, err := ReadUintValue[uint16](r)
	if err != nil {
		return SnapshotHeader{}, errors.Join(ErrSnapshotReadFailed, err)
	}
	header.Version = version

	count, err := ReadUintValue[uint64](r)
	if err != nil {
		return SnapshotHeader{}, errors.Join(ErrSnapshotReadFailed, err)
	}
	header.Count = count

	return header, nil
}

func ReadUintValue[T UintV](r io.Reader) (T, error) {
	var value T
	err := binary.Read(r, binary.LittleEndian, &value)
	return value, err
}

func ReadInt64Value(r io.Reader) (int64, error) {
	var value int64
	err := binary.Read(r, binary.LittleEndian, &value)
	return value, err
}

func ReadBytes(r io.Reader) ([]byte, error) {
	n, err := ReadUintValue[uint32](r)

	if err != nil {
		return nil, err
	}

	buf := make([]byte, n)

	_, err = io.ReadFull(r, buf)

	return buf, err
}

type Snapshot struct {
	Path    string
	SHeader SnapshotHeader
}

func NewSnapshot(path string) Snapshot {
	header := CreateSnapshotHeader(0)
	return Snapshot{Path: path, SHeader: header}
}

func (s *Snapshot) Save(storage map[string]Payload) error {
	tmpPath := s.Path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return SnapshotError{Path: tmpPath, Err: fmt.Errorf("failed to create temporary snapshot file: %w", err)}
	}

	defer func() {
		f.Close()
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	if err = s.SaveSnapshot(f, storage); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return SnapshotError{Path: tmpPath, Err: fmt.Errorf("failed to sync snapshot file: %w", err)}
	}

	if err = f.Close(); err != nil {
		return SnapshotError{Path: tmpPath, Err: fmt.Errorf("failed to close snapshot file: %w", err)}
	}

	if err = os.Rename(tmpPath, s.Path); err != nil {
		return SnapshotError{Path: s.Path, Err: fmt.Errorf("failed to rename snapshot file: %w", err)}
	}

	return nil
}

func (s *Snapshot) Load() (map[string]Payload, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, SnapshotError{Path: s.Path, Err: fmt.Errorf("failed to open snapshot file: %w", err)}
	}
	defer f.Close()

	return s.LoadSnapshot(f)
}

func (s *Snapshot) SaveSnapshot(w io.Writer, storage map[string]Payload) error {
	// header = bytes[4] + version as a uint16 + count as a uint64
	if err := WriteHeader(w, uint64(len(storage))); err != nil {
		return SnapshotError{Path: s.Path, Err: err}
	}

	// ENTRIES

	for key, value := range storage {

		// KEY
		if err := WriteBytes(w, []byte(key)); err != nil {
			return SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotWriteFailed, err)}
		}

		// VALUE
		if err := WriteBytes(w, value.Value); err != nil {
			return SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotWriteFailed, err)}
		}

		// METADATA CREATED AT
		if err := WriteInt64Value(w, value.Metadata.CreatedAt.UnixNano()); err != nil {
			return SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotWriteFailed, err)}
		}

		// METADATA EXPIRES AT

		if value.Metadata.ExpiresAt == nil {
			if err := WriteUintValue[uint8](w, uint8(0)); err != nil {
				return SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotWriteFailed, err)}
			}
		} else {
			if err := WriteUintValue[uint8](w, uint8(1)); err != nil {
				return SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotWriteFailed, err)}
			}
			if err := WriteInt64Value(w, value.Metadata.ExpiresAt.UnixNano()); err != nil {
				return SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotWriteFailed, err)}
			}
		}
	}
	return nil
}

func (s *Snapshot) LoadSnapshot(r io.Reader) (map[string]Payload, error) {
	header, err := ReadSnapshotHeader(r)
	if err != nil {
		return nil, SnapshotError{Path: s.Path, Err: err}
	}

	if err := header.Validate(); err != nil {
		return nil, SnapshotError{Path: s.Path, Err: err}
	}

	count := header.Count

	storage := make(map[string]Payload, count)

	for i := uint64(0); i < count; i++ {

		// KEY
		keyBytes, err := ReadBytes(r)
		if err != nil {
			return nil, SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotReadFailed, err)}
		}

		// VALUE
		valueBytes, err := ReadBytes(r)
		if err != nil {
			return nil, SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotReadFailed, err)}
		}

		// METADATA CREATED AT
		createdAtNano, err := ReadInt64Value(r)
		if err != nil {
			return nil, SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotReadFailed, err)}
		}

		// HAS EXPIRY METADATA
		hasExpiry, err := ReadUintValue[uint8](r)
		if err != nil {
			return nil, SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotReadFailed, err)}
		}

		var expires *time.Time

		if hasExpiry == 1 {
			expiresNano, err := ReadInt64Value(r)
			if err != nil {
				return nil, SnapshotError{Path: s.Path, Err: errors.Join(ErrSnapshotReadFailed, err)}
			}
			t := time.Unix(0, expiresNano)
			expires = &t
		}

		storage[string(keyBytes)] = Payload{
			Value: valueBytes,
			Metadata: PayloadMetadata{
				CreatedAt: time.Unix(0, createdAtNano),
				ExpiresAt: expires,
			},
		}
	}
	return storage, nil
}
