package redismq

import (
	"errors"
	"testing"
)

func TestRedisMqConfigValidate(t *testing.T) {
	t.Run("blank addr returns ErrConfigAddrBlank", func(t *testing.T) {
		err := RedisMqConfig{Group: "GID_Test", Addr: ""}.validate()
		if !errors.Is(err, ErrConfigAddrBlank) {
			t.Fatalf("expected ErrConfigAddrBlank, got %v", err)
		}
	})

	t.Run("blank group returns ErrConfigGroupBlank", func(t *testing.T) {
		err := RedisMqConfig{Group: "", Addr: "127.0.0.1:6379"}.validate()
		if !errors.Is(err, ErrConfigGroupBlank) {
			t.Fatalf("expected ErrConfigGroupBlank, got %v", err)
		}
	})

	t.Run("valid config passes validation", func(t *testing.T) {
		err := RedisMqConfig{Group: "GID_Test", Addr: "127.0.0.1:6379"}.validate()
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})
}
