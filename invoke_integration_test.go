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

	client, err := New(RedisMqConfig{Group: testGroup, Addr: testRedisAddr})
	require.NoError(t, err)

	err = client.RegisterListener(context.Background(), &stubListener{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = client.Start(ctx)
	require.NoError(t, err)

	defer closeClient(t, client)

	err = client.RegisterInvoke(ctx, "TestInvoke", func(_ context.Context, request any) (response any, err error) {
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
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	t.Run("Test Method Invoke", func(t *testing.T) {
		res := client.Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: 1,
		}, 0)
		require.NotNil(t, res)
		require.True(t, res.Status)
		t.Logf("TestRequest:%s", jsonutil.MarshalString(res))
	})
	t.Run("Test Method Invoke Error", func(t *testing.T) {
		res := client.Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: "error",
		}, 0)
		require.NotNil(t, res)
		require.False(t, res.Status)
		t.Logf("TestErrorRequest:%s", jsonutil.MarshalString(res))
	})
	t.Run("Test Method Invoke Panic", func(t *testing.T) {
		res := client.Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: "panic",
		}, 0)
		require.NotNil(t, res)
		require.False(t, res.Status)
		t.Logf("TestPanicRequest:%s", jsonutil.MarshalString(res))
	})
	t.Run("Test Method Invoke Timeout", func(t *testing.T) {
		res := client.Invoke(ctx, &InvokeRequest{
			Group:   testGroup,
			Method:  "TestInvoke",
			Request: "timeout",
		}, 0)
		require.NotNil(t, res)
		require.False(t, res.Status)
		t.Logf("TestTimeOutRequest:%s", jsonutil.MarshalString(res))
	})
}
