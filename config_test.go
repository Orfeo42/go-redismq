package redismq

import (
	"errors"
	"testing"
)

func snapshotConfig() (string, string, string, int) {
	return group, addr, password, database
}

func restoreConfig(t *testing.T, savedGroup, savedAddr, savedPassword string, savedDatabase int) {
	t.Helper()

	t.Cleanup(func() {
		group = savedGroup
		addr = savedAddr
		password = savedPassword
		database = savedDatabase
	})
}

func TestRegisterRedisMqConfig(t *testing.T) {
	t.Run("nil config returns ErrConfigNil", func(t *testing.T) {
		group, savedAddr, savedPassword, savedDatabase := snapshotConfig()
		restoreConfig(t, group, savedAddr, savedPassword, savedDatabase)

		err := RegisterRedisMqConfig(nil)
		if !errors.Is(err, ErrConfigNil) {
			t.Fatalf("expected ErrConfigNil, got %v", err)
		}
	})

	t.Run("blank addr returns ErrConfigAddrBlank", func(t *testing.T) {
		group, savedAddr, savedPassword, savedDatabase := snapshotConfig()
		restoreConfig(t, group, savedAddr, savedPassword, savedDatabase)

		err := RegisterRedisMqConfig(&RedisMqConfig{
			Group: "GID_Test",
			Addr:  "",
		})
		if !errors.Is(err, ErrConfigAddrBlank) {
			t.Fatalf("expected ErrConfigAddrBlank, got %v", err)
		}
	})

	t.Run("blank group returns ErrConfigGroupBlank", func(t *testing.T) {
		group, savedAddr, savedPassword, savedDatabase := snapshotConfig()
		restoreConfig(t, group, savedAddr, savedPassword, savedDatabase)

		err := RegisterRedisMqConfig(&RedisMqConfig{
			Group: "",
			Addr:  "127.0.0.1:6379",
		})
		if !errors.Is(err, ErrConfigGroupBlank) {
			t.Fatalf("expected ErrConfigGroupBlank, got %v", err)
		}
	})

	t.Run("valid config registers and GetRedisConfig returns matching values", func(t *testing.T) {
		group, savedAddr, savedPassword, savedDatabase := snapshotConfig()
		restoreConfig(t, group, savedAddr, savedPassword, savedDatabase)

		err := RegisterRedisMqConfig(&RedisMqConfig{
			Group:    "GID_Test",
			Addr:     "127.0.0.1:6379",
			Password: "secret",
			Database: 3,
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		options, err := getRedisConfig()
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		if options.Addr != "127.0.0.1:6379" {
			t.Fatalf("expected addr %q, got %q", "127.0.0.1:6379", options.Addr)
		}

		if options.Password != "secret" {
			t.Fatalf("expected password %q, got %q", "secret", options.Password)
		}

		if options.DB != 3 {
			t.Fatalf("expected db %d, got %d", 3, options.DB)
		}
	})

	t.Run("rejected config leaves prior state intact", func(t *testing.T) {
		group, savedAddr, savedPassword, savedDatabase := snapshotConfig()
		restoreConfig(t, group, savedAddr, savedPassword, savedDatabase)

		err := RegisterRedisMqConfig(&RedisMqConfig{
			Group:    "GID_First",
			Addr:     "127.0.0.1:7000",
			Password: "first_secret",
			Database: 1,
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		err = RegisterRedisMqConfig(nil)
		if !errors.Is(err, ErrConfigNil) {
			t.Fatalf("expected ErrConfigNil, got %v", err)
		}

		options, err := getRedisConfig()
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		if options.Addr != "127.0.0.1:7000" {
			t.Fatalf("expected addr %q, got %q", "127.0.0.1:7000", options.Addr)
		}

		if options.Password != "first_secret" {
			t.Fatalf("expected password %q, got %q", "first_secret", options.Password)
		}

		if options.DB != 1 {
			t.Fatalf("expected db %d, got %d", 1, options.DB)
		}
	})
}

func TestGetRedisConfig(t *testing.T) {
	t.Run("returns ErrConfigNotSet when addr is empty", func(t *testing.T) {
		group, savedAddr, savedPassword, savedDatabase := snapshotConfig()
		restoreConfig(t, group, savedAddr, savedPassword, savedDatabase)

		addr = ""

		_, err := getRedisConfig()
		if !errors.Is(err, ErrConfigNotSet) {
			t.Fatalf("expected ErrConfigNotSet, got %v", err)
		}
	})
}
