package multiplexer

import (
	"github.com/vpro3611/gomembase.git/pkg/persistence"
)

// ScopedWalLogger intercepts log calls from a sub-instance engine
// and prepends the sub-instance's UUID to the arguments.
type ScopedWalLogger struct {
	parent persistence.WalLogger
	uuid   string
}

func NewScopedWalLogger(parent persistence.WalLogger, uuid string) *ScopedWalLogger {
	return &ScopedWalLogger{
		parent: parent,
		uuid:   uuid,
	}
}

func (s *ScopedWalLogger) Log(engineID string, action string, args ...string) error {
	newArgs := make([]string, 0, len(args)+1)
	newArgs = append(newArgs, s.uuid)
	newArgs = append(newArgs, args...)
	return s.parent.Log(engineID, action, newArgs...)
}
