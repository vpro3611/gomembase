package core

import (
	"errors"
	"fmt"
)

var (
	KeyNotFoundError = errors.New("key was not found")
	KeyExpiredError  = errors.New("key has expired")
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
