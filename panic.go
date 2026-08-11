package go_redismq

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

func panicError(exception any) error {
	err, ok := exception.(error)
	if !ok {
		err = fmt.Errorf("redismq: panic: %v", exception)
	}

	return err
}

func newRedisClient() (*redis.Client, error) {
	options, err := GetRedisConfig()
	if err != nil {
		return nil, err
	}

	return redis.NewClient(options), nil
}
