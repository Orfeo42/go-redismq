package idgen

import (
	"regexp"
	"sync"
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

func TestConcurrentRandomAlphanumeric(t *testing.T) {
	t.Run("safe under concurrent use", func(t *testing.T) {
		const n = 100

		var wg sync.WaitGroup
		wg.Add(n)

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				if got := RandomAlphanumeric(6); len(got) != 6 {
					t.Errorf("expected length 6, got %d", len(got))
				}
			}()
		}

		wg.Wait()
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
