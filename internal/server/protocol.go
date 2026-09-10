package server

import (
	"fmt"
	"strings"
)

// command is a parsed client request line.
type command struct {
	name  string
	key   string
	value string
}

// parseCommand parses a single request line into a command. It never
// panics on malformed input; it returns an error describing the problem
// instead.
func parseCommand(line string) (command, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return command{}, fmt.Errorf("empty command")
	}

	fields := splitFields(trimmed, 3)
	name := strings.ToUpper(fields[0])

	switch name {
	case "GET", "DELETE":
		if len(fields) != 2 {
			return command{}, fmt.Errorf("%s requires exactly one key", name)
		}
		return command{name: name, key: fields[1]}, nil
	case "PUT":
		if len(fields) != 3 {
			return command{}, fmt.Errorf("PUT requires a key and a value")
		}
		return command{name: name, key: fields[1], value: fields[2]}, nil
	default:
		return command{}, fmt.Errorf("unknown command %q", fields[0])
	}
}

// splitFields splits s into at most n whitespace-separated fields. The
// final field contains the remainder of the string with its internal
// spacing preserved, which lets PUT values contain spaces.
func splitFields(s string, n int) []string {
	fields := make([]string, 0, n)
	for len(fields) < n-1 {
		s = strings.TrimLeft(s, " \t")
		if s == "" {
			break
		}
		idx := strings.IndexAny(s, " \t")
		if idx == -1 {
			fields = append(fields, s)
			s = ""
			break
		}
		fields = append(fields, s[:idx])
		s = s[idx:]
	}
	s = strings.TrimLeft(s, " \t")
	if s != "" {
		fields = append(fields, s)
	}
	return fields
}
