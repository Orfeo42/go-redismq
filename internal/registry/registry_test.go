package registry

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/Orfeo42/go-redismq/v3/internal/mqtype"
)

type fakeAttrLogger struct {
	calls []string
}

func (f *fakeAttrLogger) LogAttrs(_ context.Context, _ slog.Level, msg string, _ ...slog.Attr) {
	f.calls = append(f.calls, msg)
}

type fakeListener struct {
	topic string
	tag   string
}

func (l *fakeListener) GetTopic() string { return l.topic }

func (l *fakeListener) GetTag() string { return l.tag }

func (l *fakeListener) Consume(_ context.Context, _ *mqtype.Message) mqtype.Action {
	return mqtype.CommitMessage
}

type fakeChecker struct {
	topic string
	tag   string
}

func (c *fakeChecker) GetTopic() string { return c.topic }

func (c *fakeChecker) GetTag() string { return c.tag }

func (c *fakeChecker) Checker(_ *mqtype.Message) mqtype.TransactionStatus {
	return mqtype.CommitTransaction
}

func TestRegisterListener(t *testing.T) {
	t.Run("registers a valid listener", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterListener(context.Background(), &fakeListener{topic: "t1", tag: "tag1"})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		got := r.GetListenerFor("t1", "tag1")
		if got == nil {
			t.Fatal("expected listener to be registered")
		}
	})

	t.Run("rejects nil listener", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterListener(context.Background(), nil)
		if !errors.Is(err, ErrNilListener) {
			t.Fatalf("expected ErrNilListener, got %v", err)
		}

		if r.TopicCount() != 0 {
			t.Fatalf("expected 0 topics, got %d", r.TopicCount())
		}
	})

	t.Run("rejects invalid topic", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterListener(context.Background(), &fakeListener{topic: "*", tag: "tag1"})
		if !errors.Is(err, ErrInvalidTopic) {
			t.Fatalf("expected ErrInvalidTopic, got %v", err)
		}

		if r.TopicCount() != 0 {
			t.Fatalf("expected 0 topics, got %d", r.TopicCount())
		}
	})

	t.Run("rejects duplicate listener for the same message key", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterListener(context.Background(), &fakeListener{topic: "t1", tag: "tag1"})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		err = r.RegisterListener(context.Background(), &fakeListener{topic: "t1", tag: "tag1"})
		if !errors.Is(err, ErrDuplicateListener) {
			t.Fatalf("expected ErrDuplicateListener, got %v", err)
		}

		if r.TopicCount() != 1 {
			t.Fatalf("expected 1 topic, got %d", r.TopicCount())
		}
	})
}

func TestRegisterChecker(t *testing.T) {
	t.Run("registers a valid checker", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterChecker(context.Background(), &fakeChecker{topic: "t1", tag: "tag1"})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		got := r.GetCheckerFor("t1", "tag1")
		if got == nil {
			t.Fatal("expected checker to be registered")
		}
	})

	t.Run("rejects nil checker", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterChecker(context.Background(), nil)
		if !errors.Is(err, ErrNilChecker) {
			t.Fatalf("expected ErrNilChecker, got %v", err)
		}

		if len(r.SnapshotCheckers()) != 0 {
			t.Fatalf("expected 0 checkers, got %d", len(r.SnapshotCheckers()))
		}
	})

	t.Run("rejects duplicate checker for the same message key", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		err := r.RegisterChecker(context.Background(), &fakeChecker{topic: "t1", tag: "tag1"})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		err = r.RegisterChecker(context.Background(), &fakeChecker{topic: "t1", tag: "tag1"})
		if !errors.Is(err, ErrDuplicateChecker) {
			t.Fatalf("expected ErrDuplicateChecker, got %v", err)
		}

		if len(r.SnapshotCheckers()) != 1 {
			t.Fatalf("expected 1 checker, got %d", len(r.SnapshotCheckers()))
		}
	})
}

func TestSnapshotListeners(t *testing.T) {
	fl := &fakeAttrLogger{}
	r := New(fl)

	_ = r.RegisterListener(context.Background(), &fakeListener{topic: "t1", tag: "tag1"})

	snap := r.SnapshotListeners()
	if len(snap) != 1 {
		t.Fatalf("expected 1 listener, got %d", len(snap))
	}
}

func TestGetTopics(t *testing.T) {
	fl := &fakeAttrLogger{}
	r := New(fl)

	_ = r.RegisterListener(context.Background(), &fakeListener{topic: "t1", tag: "tag1"})
	_ = r.RegisterListener(context.Background(), &fakeListener{topic: "t2", tag: "tag1"})

	topics := r.GetTopics()
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
}

func TestIsValidTopic(t *testing.T) {
	t.Run("empty topic is invalid", func(t *testing.T) {
		if IsValidTopic("") {
			t.Fatal("expected empty topic to be invalid")
		}
	})

	t.Run("wildcard topic is invalid", func(t *testing.T) {
		if IsValidTopic("*") {
			t.Fatal("expected wildcard topic to be invalid")
		}
	})

	t.Run("regular topic is valid", func(t *testing.T) {
		if !IsValidTopic("t1") {
			t.Fatal("expected regular topic to be valid")
		}
	})
}

func TestConcurrentRegisterListenerAndSnapshot(t *testing.T) {
	t.Run("RegisterListener and SnapshotListeners run concurrently without racing", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		const n = 50

		done := make(chan struct{}, n*2)

		for i := 0; i < n; i++ {
			go func(i int) {
				_ = r.RegisterListener(context.Background(), &fakeListener{topic: "topic", tag: "tag"})

				done <- struct{}{}
			}(i)
		}

		for i := 0; i < n; i++ {
			go func() {
				_ = r.SnapshotListeners()

				done <- struct{}{}
			}()
		}

		for i := 0; i < n*2; i++ {
			<-done
		}
	})
}

func TestConcurrentRegisterCheckerAndSnapshot(t *testing.T) {
	t.Run("RegisterChecker and SnapshotCheckers run concurrently without racing", func(t *testing.T) {
		fl := &fakeAttrLogger{}
		r := New(fl)

		const n = 50

		done := make(chan struct{}, n*2)

		for i := 0; i < n; i++ {
			go func(i int) {
				_ = r.RegisterChecker(context.Background(), &fakeChecker{topic: "topic", tag: "tag"})

				done <- struct{}{}
			}(i)
		}

		for i := 0; i < n; i++ {
			go func() {
				_ = r.SnapshotCheckers()

				done <- struct{}{}
			}()
		}

		for i := 0; i < n*2; i++ {
			<-done
		}
	})
}
