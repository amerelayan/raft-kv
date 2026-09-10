package server

import "testing"

func TestParseCommandGet(t *testing.T) {
	cmd, err := parseCommand("GET foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.name != "GET" || cmd.key != "foo" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseCommandDelete(t *testing.T) {
	cmd, err := parseCommand("DELETE foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.name != "DELETE" || cmd.key != "foo" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseCommandPut(t *testing.T) {
	cmd, err := parseCommand("PUT foo bar")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.name != "PUT" || cmd.key != "foo" || cmd.value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseCommandPutValueWithSpaces(t *testing.T) {
	cmd, err := parseCommand("PUT foo bar baz  qux")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.value != "bar baz  qux" {
		t.Fatalf("expected value to preserve internal spacing, got %q", cmd.value)
	}
}

func TestParseCommandCaseInsensitiveName(t *testing.T) {
	cmd, err := parseCommand("get foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.name != "GET" {
		t.Fatalf("expected command name to be normalized to GET, got %q", cmd.name)
	}
}

func TestParseCommandExtraWhitespace(t *testing.T) {
	cmd, err := parseCommand("   GET    foo   ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.key != "foo" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseCommandEmptyLine(t *testing.T) {
	if _, err := parseCommand(""); err == nil {
		t.Fatal("expected error for empty command")
	}
	if _, err := parseCommand("   "); err == nil {
		t.Fatal("expected error for whitespace-only command")
	}
}

func TestParseCommandUnknown(t *testing.T) {
	if _, err := parseCommand("FOO bar"); err == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestParseCommandGetMissingKey(t *testing.T) {
	if _, err := parseCommand("GET"); err == nil {
		t.Fatal("expected error for GET with no key")
	}
}

func TestParseCommandDeleteMissingKey(t *testing.T) {
	if _, err := parseCommand("DELETE"); err == nil {
		t.Fatal("expected error for DELETE with no key")
	}
}

func TestParseCommandPutMissingValue(t *testing.T) {
	if _, err := parseCommand("PUT foo"); err == nil {
		t.Fatal("expected error for PUT with no value")
	}
}

func TestParseCommandPutMissingKeyAndValue(t *testing.T) {
	if _, err := parseCommand("PUT"); err == nil {
		t.Fatal("expected error for PUT with no key or value")
	}
}
