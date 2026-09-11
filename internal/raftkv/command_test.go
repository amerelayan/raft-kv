package raftkv

import "testing"

func TestEncodeDecodeCommandPut(t *testing.T) {
	original := command{Op: opPut, Key: "foo", Value: "bar"}

	data, err := encodeCommand(original)
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}

	got, err := decodeCommand(data)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if got != original {
		t.Fatalf("decoded = %+v, want %+v", got, original)
	}
}

func TestEncodeDecodeCommandDelete(t *testing.T) {
	original := command{Op: opDelete, Key: "foo"}

	data, err := encodeCommand(original)
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}

	got, err := decodeCommand(data)
	if err != nil {
		t.Fatalf("decodeCommand: %v", err)
	}
	if got != original {
		t.Fatalf("decoded = %+v, want %+v", got, original)
	}
}

func TestDecodeCommandRejectsMalformedJSON(t *testing.T) {
	if _, err := decodeCommand([]byte("not json")); err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
}

func TestDecodeCommandRejectsUnknownOp(t *testing.T) {
	data, err := encodeCommand(command{Op: "FROBNICATE", Key: "foo"})
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	if _, err := decodeCommand(data); err == nil {
		t.Fatal("expected an error for an unknown op")
	}
}

func TestDecodeCommandRejectsEmptyKey(t *testing.T) {
	data, err := encodeCommand(command{Op: opPut, Key: "", Value: "bar"})
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	if _, err := decodeCommand(data); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}
