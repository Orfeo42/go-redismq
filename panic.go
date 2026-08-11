package redismq

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
	configMu.RLock()
	defer configMu.RUnlock()

	if len(addr) == 0 {
		return nil, ErrConfigNotSet
	}

	return redisClient, nil
}
