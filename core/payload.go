package core

import "time"

type PayloadMetadata struct {
	CreatedAt time.Time
	ExpiresAt *time.Time
}

func NewPayloadMetadata(createdAt time.Time, expiresAt *time.Time) PayloadMetadata {
	return PayloadMetadata{CreatedAt: createdAt, ExpiresAt: expiresAt}
}

func (pm *PayloadMetadata) IsExpired() bool {
	return pm.ExpiresAt != nil && pm.ExpiresAt.Before(time.Now())
}

type Payload struct {
	Value    []byte
	Metadata PayloadMetadata
}
