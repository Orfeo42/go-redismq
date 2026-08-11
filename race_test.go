package redismq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/Orfeo42/go-redismq/v3/internal/idgen"
)

type raceTestListener struct {
	topic string
	tag   string
}

func (l *raceTestListener) GetTopic() string { return l.topic }

func (l *raceTestListener) GetTag() string { return l.tag }

func (l *raceTestListener) Consume(_ context.Context, _ *Message) Action { return CommitMessage }

type raceTestChecker struct {
	topic string
	tag   string
}

func (c *raceTestChecker) GetTopic() string { return c.topic }

func (c *raceTestChecker) GetTag() string { return c.tag }

func (c *raceTestChecker) Checker(_ *Message) TransactionStatus { return CommitTransaction }

func installRaceSafeTestLogger(t *testing.T) {
	t.Helper()

	original := GetLogger()

	t.Cleanup(func() {
		if original == nil {
			return
		}

		SetLogger(original)
	})

	SetSlogLogger(slog.New(slog.DiscardHandler))
}

func TestConcurrentListenerRegistrationAndLookup(t *testing.T) {
	t.Run("RegisterListener and Listeners run concurrently without racing", func(t *testing.T) {
		installRaceSafeTestLogger(t)

		listenerMu.Lock()
		originalListeners := listeners
		originalTopics := topics
		listeners = map[string]IMessageListener{}
		topics = make([]string, 0, 100)
		listenerMu.Unlock()

		t.Cleanup(func() {
			listenerMu.Lock()
			listeners = originalListeners
			topics = originalTopics
			listenerMu.Unlock()
		})

		const n = 50

		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				RegisterListener(context.Background(), &raceTestListener{topic: fmt.Sprintf("topic-%d", i), tag: "tag"})
			}(i)
		}

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				_ = snapshotListeners()
			}()
		}

		wg.Wait()
	})
}

func TestConcurrentCheckerRegistrationAndLookup(t *testing.T) {
	t.Run("RegisterChecker and Checkers run concurrently without racing", func(t *testing.T) {
		installRaceSafeTestLogger(t)

		checkerMu.Lock()
		originalCheckers := checkers
		checkers = map[string]IMessageChecker{}
		checkerMu.Unlock()

		t.Cleanup(func() {
			checkerMu.Lock()
			checkers = originalCheckers
			checkerMu.Unlock()
		})

		const n = 50

		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				RegisterChecker(context.Background(), &raceTestChecker{topic: fmt.Sprintf("topic-%d", i), tag: "tag"})
			}(i)
		}

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				_ = snapshotCheckers()
			}()
		}

		wg.Wait()
	})
}

func TestConcurrentInvokeRegistrationAndLookup(t *testing.T) {
	t.Run("RegisterInvoke and invoke lookup run concurrently without racing", func(t *testing.T) {
		installRaceSafeTestLogger(t)

		invokeMu.Lock()
		originalInvokeMap := invokeMap
		invokeMap = make(map[string]func(ctx context.Context, request any) (response any, err error))
		invokeMu.Unlock()

		t.Cleanup(func() {
			invokeMu.Lock()
			invokeMap = originalInvokeMap
			invokeMu.Unlock()
		})

		const n = 50

		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				method := fmt.Sprintf("method-%d", i)
				RegisterInvoke(context.Background(), method, func(_ context.Context, request any) (any, error) {
					return request, nil
				})
			}(i)
		}

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				_, _ = getInvoke(fmt.Sprintf("method-%d", i))
			}(i)
		}

		wg.Wait()
	})
}

func TestConcurrentSetLoggerAndLogAttrs(t *testing.T) {
	t.Run("SetLogger and logAttrs run concurrently without racing", func(t *testing.T) {
		original := GetLogger()

		t.Cleanup(func() { SetLogger(original) })

		const n = 50

		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				SetSlogLogger(slog.New(slog.DiscardHandler))
			}()
		}

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				logAttrs(context.Background(), slog.LevelInfo, "redismq: race test log", slog.String("case", "concurrent"))
			}()
		}

		wg.Wait()
	})
}

func TestConcurrentSetTraceIDFromContextAndStampTraceID(t *testing.T) {
	t.Run("SetTraceIDFromContext and stampTraceID run concurrently without racing", func(t *testing.T) {
		original := getTraceIDFromContext()

		t.Cleanup(func() { SetTraceIDFromContext(original) })

		const n = 50

		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()

				SetTraceIDFromContext(func(_ context.Context) string {
					return fmt.Sprintf("trace-%d", i)
				})
			}(i)
		}

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				message := &Message{Topic: "t", Tag: "tag1"}
				stampTraceID(context.Background(), message)
			}()
		}

		wg.Wait()
	})
}

func TestConcurrentGenerateRandomAlphanumeric(t *testing.T) {
	t.Run("GenerateRandomAlphanumeric is safe under concurrent use", func(t *testing.T) {
		const n = 100

		var wg sync.WaitGroup
		wg.Add(n)

		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				if got := idgen.RandomAlphanumeric(6); len(got) != 6 {
					t.Errorf("expected length 6, got %d", len(got))
				}
			}()
		}

		wg.Wait()
	})
}
