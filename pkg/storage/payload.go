package storage

import "time"

type PayloadMetadata struct {
	createdAt time.Time
	expiresAt *time.Time
}

func NewPayloadMetadata(createdAt time.Time, expiresAt *time.Time) PayloadMetadata {
	return PayloadMetadata{createdAt: createdAt, expiresAt: expiresAt}
}

func (pm PayloadMetadata) CreatedAt() time.Time {
	return pm.createdAt
}

func (pm PayloadMetadata) ExpiresAt() *time.Time {
	return pm.expiresAt
}

func (pm PayloadMetadata) IsExpired() bool {
	return pm.expiresAt != nil && pm.expiresAt.Before(time.Now())
}

type Payload struct {
	value    []byte
	metadata PayloadMetadata
}

func NewPayload(value []byte, metadata PayloadMetadata) Payload {
	return Payload{value: value, metadata: metadata}
}

func (p Payload) Value() []byte {
	return p.value
}

func (p Payload) Metadata() PayloadMetadata {
	return p.metadata
}
