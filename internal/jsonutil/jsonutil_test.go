package jsonutil

import (
	"errors"
	"testing"
)

type sample struct {
	Name string `json:"name"`
}

func TestMarshalString(t *testing.T) {
	t.Run("nil target returns empty string", func(t *testing.T) {
		got := MarshalString(nil)
		if got != "" {
			t.Fatalf("expected empty string, got %q", got)
		}
	})

	t.Run("valid target returns json string", func(t *testing.T) {
		got := MarshalString(sample{Name: "foo"})

		want := `{"name":"foo"}`
		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})

	t.Run("unmarshalable target returns empty string", func(t *testing.T) {
		got := MarshalString(make(chan int))
		if got != "" {
			t.Fatalf("expected empty string, got %q", got)
		}
	})
}

func TestUnmarshalString(t *testing.T) {
	t.Run("empty target returns ErrTargetNil", func(t *testing.T) {
		var s sample

		err := UnmarshalString("", &s)
		if !errors.Is(err, ErrTargetNil) {
			t.Fatalf("expected ErrTargetNil, got %v", err)
		}
	})

	t.Run("valid json unmarshals into target", func(t *testing.T) {
		var s sample

		err := UnmarshalString(`{"name":"bar"}`, &s)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if s.Name != "bar" {
			t.Fatalf("expected Name to be bar, got %q", s.Name)
		}
	})

	t.Run("invalid json returns error", func(t *testing.T) {
		var s sample

		err := UnmarshalString("not-json", &s)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestStringPtr(t *testing.T) {
	t.Run("returns pointer to given value", func(t *testing.T) {
		got := StringPtr("hello")
		if got == nil {
			t.Fatal("expected non-nil pointer")
		}

		if *got != "hello" {
			t.Fatalf("expected hello, got %q", *got)
		}
	})
}
