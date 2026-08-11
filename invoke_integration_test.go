package redismq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Orfeo42/go-redismq/v3/internal/jsonutil"
)

func TestMethodInvoke(t *testing.T) {
	requireRedis(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	savedGroup, savedAddr, savedPassword, savedDatabase := snapshotConfig()
	restoreConfig(t, savedGroup, savedAddr, savedPassword, savedDatabase)

	listenerMu.Lock()
	originalListeners := listeners
	originalTopics := topics
	listenerMu.Unlock()

	t.Cleanup(func() {
		listenerMu.Lock()
		listeners = originalListeners
		topics = originalTopics
		listenerMu.Unlock()
	})

	invokeMu.Lock()
	originalInvokeMap := invokeMap
	invokeMap = make(map[string]func(ctx context.Context, request any) (response any, err error))
	invokeMu.Unlock()

	t.Cleanup(func() {
		invokeMu.Lock()
		invokeMap = originalInvokeMap
		invokeMu.Unlock()
	})

	err := RegisterRedisMqConfig(context.Background(), &RedisMqConfig{
		Group:    testGroup,
		Addr:     "127.0.0.1:6379",
		Password: "",
		Database: 0,
	})
	require.NoError(t, err)
	RegisterListener(ctx, &stubListener{})
	RegisterInternalListeners(ctx)
	StartRedisMqConsumer(ctx)

	RegisterInvoke(ctx, "TestInvoke", func(ctx context.Context, request any) (response any, err error) {
		switch request {
		case "error":
			return nil, errors.New("error")
		case "panic":
			panic("panic")
		case "timeout":
			time.Sleep(30 * time.Second)

			return nil, errors.New("timeout")
		default:
			return jsonutil.MarshalString(request) + ":TestResponse", nil
		}
	})
	time.Sleep(5 * time.Second)
	t.Run("Test Method Invoke", func(t *testing.T) {
		res := Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: 1,
		}, 0)
		require.NotNil(t, res)
		require.True(t, res.Status)
		t.Logf("TestRequest:%s", jsonutil.MarshalString(res))
	})
	t.Run("Test Method Invoke Error", func(t *testing.T) {
		res := Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: "error",
		}, 0)
		require.NotNil(t, res)
		require.False(t, res.Status)
		t.Logf("TestErrorRequest:%s", jsonutil.MarshalString(res))
	})
	t.Run("Test Method Invoke Panic", func(t *testing.T) {
		res := Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: "panic",
		}, 0)
		require.NotNil(t, res)
		require.False(t, res.Status)
		t.Logf("TestPanicRequest:%s", jsonutil.MarshalString(res))
	})
	t.Run("Test Method Invoke Timeout", func(t *testing.T) {
		res := Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: "timeout",
		}, 0)
		require.NotNil(t, res)
		require.False(t, res.Status)
		t.Logf("TestTimeOutRequest:%s", jsonutil.MarshalString(res))
	})
}
