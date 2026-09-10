package store

import (
	"sync"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	s := New()

	if err := s.Put("foo", "bar"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	value, ok, err := s.Get("foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected key %q to exist", "foo")
	}
	if value != "bar" {
		t.Fatalf("expected value %q, got %q", "bar", value)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := New()

	_, ok, err := s.Get("missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected key %q to not exist", "missing")
	}
}

func TestPutOverwritesExistingKey(t *testing.T) {
	s := New()

	s.Put("foo", "bar")
	s.Put("foo", "baz")

	value, ok, err := s.Get("foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected key %q to exist", "foo")
	}
	if value != "baz" {
		t.Fatalf("expected value %q, got %q", "baz", value)
	}
}

func TestDelete(t *testing.T) {
	s := New()

	s.Put("foo", "bar")
	if err := s.Delete("foo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok, _ := s.Get("foo")
	if ok {
		t.Fatalf("expected key %q to be deleted", "foo")
	}
}

func TestDeleteMissingKeyIsNoOp(t *testing.T) {
	s := New()

	if err := s.Delete("missing"); err != nil { // should not panic or error
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Put("key", "value")
			s.Get("key")
			s.Delete("key")
		}(i)
	}

	wg.Wait()
}
