package test

import (
	"context"
	"errors"
	"fmt"
	goredismq "github.com/jackyang-hk/go-redismq"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMethodInvoke(t *testing.T) {
	goredismq.RegisterRedisMqConfig(&goredismq.RedisMqConfig{
		Group:    TestGroup,
		Addr:     "127.0.0.1:6379",
		Password: "",
		Database: 0,
	})
	goredismq.RegisterListener(&TestListener{})
	goredismq.StartRedisMqConsumer()
	ctx := context.Background()
	goredismq.RegisterInvoke("TestInvoke", func(ctx context.Context, request string) (response string, err error) {
		if request == "error" {
			return "", errors.New("error")
		} else if request == "panic" {
			panic("panic")
		} else if request == "timeout" {
			time.Sleep(30 * time.Second)
			return "", errors.New("timeout")
		} else {
			return fmt.Sprintf("%s:TestResponse", request), nil
		}
	})
	t.Run("Test Method Invoke", func(t *testing.T) {
		res := goredismq.Invoke(ctx, &goredismq.InvoiceRequest{
			Group:   TestGroup,
			Method:  "TestInvoke",
			Request: "",
		}, 0)
		require.NotNil(t, res)
		require.Equal(t, res.Status, true)
		fmt.Printf("TestRequest:%s\n", goredismq.MarshalToJsonString(res))

	})
	t.Run("Test Method Invoke Error", func(t *testing.T) {
		res := goredismq.Invoke(ctx, &goredismq.InvoiceRequest{
			Group:   TestGroup,
			Method:  "TestInvoke",
			Request: "error",
		}, 0)
		require.NotNil(t, res)
		require.Equal(t, res.Status, false)
		fmt.Printf("TestErrorRequest:%s\n", goredismq.MarshalToJsonString(res))
	})
	t.Run("Test Method Invoke Panic", func(t *testing.T) {
		res := goredismq.Invoke(ctx, &goredismq.InvoiceRequest{
			Group:   TestGroup,
			Method:  "TestInvoke",
			Request: "panic",
		}, 0)
		require.NotNil(t, res)
		require.Equal(t, res.Status, false)
		fmt.Printf("TestPanicRequest:%s\n", goredismq.MarshalToJsonString(res))
	})
	t.Run("Test Method Invoke Timeout", func(t *testing.T) {
		res := goredismq.Invoke(ctx, &goredismq.InvoiceRequest{
			Group:   TestGroup,
			Method:  "TestInvoke",
			Request: "timeout",
		}, 0)
		require.NotNil(t, res)
		require.Equal(t, res.Status, false)
		fmt.Printf("TestTimeOutRequest:%s\n", goredismq.MarshalToJsonString(res))
	})
}
