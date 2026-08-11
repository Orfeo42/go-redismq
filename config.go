package go_redismq

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

var Group = "GID_Default"
var addr = ""
var password = ""
var database = 0

var (
	ErrConfigNil        = errors.New("redismq: config is nil")
	ErrConfigAddrBlank  = errors.New("redismq: config addr is blank")
	ErrConfigGroupBlank = errors.New("redismq: config group is blank")
	ErrConfigNotSet     = errors.New("redismq: redis config not registered")
)

type RedisMqConfig struct {
	Group    string
	Addr     string
	Password string
	Database int
}

func RegisterRedisMqConfig(one *RedisMqConfig) error {
	if one == nil {
		return ErrConfigNil
	}

	if len(one.Addr) == 0 {
		return ErrConfigAddrBlank
	}

	if len(one.Group) == 0 {
		return ErrConfigGroupBlank
	}

	Group = one.Group
	addr = one.Addr
	password = one.Password

	if one.Database >= 0 {
		database = one.Database
	}

	return nil
}

func GetRedisConfig() (*redis.Options, error) {
	if len(addr) == 0 {
		return nil, ErrConfigNotSet
	}

	return &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       database,
	}, nil
}
