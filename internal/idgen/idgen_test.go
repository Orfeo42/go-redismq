package idgen

import (
	"regexp"
	"testing"
)

func TestRandomAlphanumeric(t *testing.T) {
	t.Run("returns string of requested length", func(t *testing.T) {
		got := RandomAlphanumeric(12)
		if len(got) != 12 {
			t.Fatalf("expected length 12, got %d", len(got))
		}
	})

	t.Run("returns only alphanumeric characters", func(t *testing.T) {
		got := RandomAlphanumeric(64)

		matched := regexp.MustCompile(`^[a-zA-Z0-9]+$`).MatchString(got)
		if !matched {
			t.Fatalf("expected alphanumeric string, got %q", got)
		}
	})

	t.Run("zero length returns empty string", func(t *testing.T) {
		got := RandomAlphanumeric(0)
		if got != "" {
			t.Fatalf("expected empty string, got %q", got)
		}
	})
}

func TestUniqueNo(t *testing.T) {
	t.Run("contains district code prefix", func(t *testing.T) {
		got := UniqueNo("topicA")

		matched := regexp.MustCompile(`^MQCT_topicA_[a-f0-9]{32}$`).MatchString(got)
		if !matched {
			t.Fatalf("expected format MQCT_topicA_<hex32>, got %q", got)
		}
	})

	t.Run("successive calls return unique values", func(t *testing.T) {
		first := UniqueNo("topicA")
		second := UniqueNo("topicA")

		if first == second {
			t.Fatalf("expected unique values, got %q twice", first)
		}
	})
}
