package raftkv

import (
	"encoding/json"
	"fmt"
)

// opType identifies the kind of replicated operation a command performs.
type opType string

const (
	opPut    opType = "PUT"
	opDelete opType = "DELETE"
)

// command is the deterministic, replicated representation of a client
// write, encoded as the []byte payload of a raft.LogEntry.Command. JSON
// keeps it simple, readable in logs and tests, and easy to extend with
// new fields or operation types later without changing the encoding
// scheme itself. GET is never encoded as a command: it is a local read,
// never replicated.
type command struct {
	Op    opType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // unused for DELETE
}

func encodeCommand(cmd command) ([]byte, error) {
	return json.Marshal(cmd)
}

// decodeCommand decodes and validates data, returning an error for
// anything malformed rather than a zero-value command that might be
// silently misapplied.
func decodeCommand(data []byte) (command, error) {
	var cmd command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return command{}, fmt.Errorf("raftkv: decode command: %w", err)
	}
	if err := cmd.validate(); err != nil {
		return command{}, err
	}
	return cmd, nil
}

func (c command) validate() error {
	switch c.Op {
	case opPut, opDelete:
	default:
		return fmt.Errorf("raftkv: unknown op %q", c.Op)
	}
	if c.Key == "" {
		return fmt.Errorf("raftkv: empty key")
	}
	return nil
}
