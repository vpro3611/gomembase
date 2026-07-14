package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	pkgerrors "github.com/vpro3611/gomembase.git/pkg/errors"
	"github.com/vpro3611/gomembase.git/pkg/storage"
	"io"
	"os"
	"time"
)

var (
	FirstBytes = [4]byte{'R', 'C', 'D', 'B'}
	Version    = uint16(1)
)

type SnapshotHeader struct {
	firstBytes [4]byte
	version    uint16
	count      uint64
}

func (sh *SnapshotHeader) FirstBytes() [4]byte {
	return sh.firstBytes
}

func (sh *SnapshotHeader) Version() uint16 {
	return sh.version
}

func (sh *SnapshotHeader) Count() uint64 {
	return sh.count
}

func CreateSnapshotHeader(count uint64) SnapshotHeader {
	return SnapshotHeader{firstBytes: FirstBytes, version: Version, count: count}
}

func (sh *SnapshotHeader) Validate() error {
	expected := CreateSnapshotHeader(sh.count)
	if sh.firstBytes != expected.firstBytes {
		return pkgerrors.ErrInvalidSnapshotMagic
	}
	if sh.version != expected.version {
		return pkgerrors.ErrInvalidSnapshotVersion
	}
	return nil
}

type UintV interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func WriteHeader(w io.Writer, count uint64) error {
	header := CreateSnapshotHeader(count)

	if _, err := w.Write(header.firstBytes[:]); err != nil {
		return errors.Join(pkgerrors.ErrSnapshotWriteFailed, err)
	}

	if err := WriteUintValue(w, header.version); err != nil {
		return errors.Join(pkgerrors.ErrSnapshotWriteFailed, err)
	}

	if err := WriteUintValue(w, header.count); err != nil {
		return errors.Join(pkgerrors.ErrSnapshotWriteFailed, err)
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
	if _, err := io.ReadFull(r, header.firstBytes[:]); err != nil {
		return SnapshotHeader{}, errors.Join(pkgerrors.ErrSnapshotReadFailed, err)
	}
	version, err := ReadUintValue[uint16](r)
	if err != nil {
		return SnapshotHeader{}, errors.Join(pkgerrors.ErrSnapshotReadFailed, err)
	}
	header.version = version

	count, err := ReadUintValue[uint64](r)
	if err != nil {
		return SnapshotHeader{}, errors.Join(pkgerrors.ErrSnapshotReadFailed, err)
	}
	header.count = count

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
	path    string
	sHeader SnapshotHeader
}

func NewSnapshot(path string) Snapshot {
	header := CreateSnapshotHeader(0)
	return Snapshot{path: path, sHeader: header}
}

func (s *Snapshot) Path() string {
	return s.path
}

func (s *Snapshot) SHeader() SnapshotHeader {
	return s.sHeader
}

func (s *Snapshot) Save(data map[string]storage.Payload) error {
	tmpPath := s.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return pkgerrors.SnapshotError{Path: tmpPath, Err: fmt.Errorf("failed to create temporary snapshot file: %w", err), Operation: "CREATE"}
	}

	defer func() {
		f.Close()
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	if err = s.SaveSnapshot(f, data); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return pkgerrors.SnapshotError{Path: tmpPath, Err: fmt.Errorf("failed to sync snapshot file: %w", err), Operation: "SYNC"}
	}

	if err = f.Close(); err != nil {
		return pkgerrors.SnapshotError{Path: tmpPath, Err: fmt.Errorf("failed to close snapshot file: %w", err), Operation: "CLOSE"}
	}

	if err = os.Rename(tmpPath, s.path); err != nil {
		return pkgerrors.SnapshotError{Path: s.path, Err: fmt.Errorf("failed to rename snapshot file: %w", err), Operation: "RENAME"}
	}

	return nil
}

func (s *Snapshot) Delete() error {
	err := os.Remove(s.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return pkgerrors.SnapshotError{Path: s.path, Err: fmt.Errorf("failed to delete snapshot file: %w", err), Operation: "DELETE"}
	}
	return nil
}

func (s *Snapshot) Load() (map[string]storage.Payload, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, pkgerrors.SnapshotError{Path: s.path, Err: fmt.Errorf("failed to open snapshot file: %w", err), Operation: "OPEN"}
	}
	defer f.Close()

	return s.LoadSnapshot(f)
}

func (s *Snapshot) SaveSnapshot(w io.Writer, data map[string]storage.Payload) error {
	// header = bytes[4] + version as a uint16 + count as a uint64
	if err := WriteHeader(w, uint64(len(data))); err != nil {
		return pkgerrors.SnapshotError{Path: s.path, Err: err, Operation: "SAVE"}
	}

	// ENTRIES

	for key, payload := range data {

		// KEY
		if err := WriteBytes(w, []byte(key)); err != nil {
			return pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotWriteFailed, err), Operation: "WRITE"}
		}

		// VALUE
		if err := WriteBytes(w, payload.Value()); err != nil {
			return pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotWriteFailed, err), Operation: "WRITE"}
		}

		// METADATA CREATED AT
		if err := WriteInt64Value(w, payload.Metadata().CreatedAt().UnixNano()); err != nil {
			return pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotWriteFailed, err), Operation: "WRITE"}
		}

		// METADATA EXPIRES AT

		if payload.Metadata().ExpiresAt() == nil {
			if err := WriteUintValue[uint8](w, uint8(0)); err != nil {
				return pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotWriteFailed, err), Operation: "WRITE"}
			}
		} else {
			if err := WriteUintValue[uint8](w, uint8(1)); err != nil {
				return pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotWriteFailed, err), Operation: "WRITE"}
			}
			if err := WriteInt64Value(w, payload.Metadata().ExpiresAt().UnixNano()); err != nil {
				return pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotWriteFailed, err), Operation: "WRITE"}
			}
		}
	}
	return nil
}

func (s *Snapshot) LoadSnapshot(r io.Reader) (map[string]storage.Payload, error) {
	header, err := ReadSnapshotHeader(r)
	if err != nil {
		return nil, pkgerrors.SnapshotError{Path: s.path, Err: err, Operation: "LOAD"}
	}

	if err := header.Validate(); err != nil {
		return nil, pkgerrors.SnapshotError{Path: s.path, Err: err, Operation: "LOAD"}
	}

	count := header.count

	data := make(map[string]storage.Payload, count)

	for i := uint64(0); i < count; i++ {

		// KEY
		keyBytes, err := ReadBytes(r)
		if err != nil {
			return nil, pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotReadFailed, err), Operation: "READ"}
		}

		// VALUE
		valueBytes, err := ReadBytes(r)
		if err != nil {
			return nil, pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotReadFailed, err), Operation: "READ"}
		}

		// METADATA CREATED AT
		createdAtNano, err := ReadInt64Value(r)
		if err != nil {
			return nil, pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotReadFailed, err), Operation: "READ"}
		}

		// HAS EXPIRY METADATA
		hasExpiry, err := ReadUintValue[uint8](r)
		if err != nil {
			return nil, pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotReadFailed, err), Operation: "READ"}
		}

		var expires *time.Time

		if hasExpiry == 1 {
			expiresNano, err := ReadInt64Value(r)
			if err != nil {
				return nil, pkgerrors.SnapshotError{Path: s.path, Err: errors.Join(pkgerrors.ErrSnapshotReadFailed, err), Operation: "READ"}
			}
			t := time.Unix(0, expiresNano)
			expires = &t
		}

		metadata := storage.NewPayloadMetadata(time.Unix(0, createdAtNano), expires)
		data[string(keyBytes)] = storage.NewPayload(valueBytes, metadata)
	}
	return data, nil
}
