package jsonutil

import (
	"encoding/json"
	"errors"
)

var ErrTargetNil = errors.New("redismq: target is nil")

func MarshalString(target any) string {
	if target == nil {
		return ""
	}

	marshal, err := json.Marshal(target)
	if err != nil {
		return ""
	}

	return string(marshal)
}

func UnmarshalString(target string, one any) error {
	if len(target) == 0 {
		return ErrTargetNil
	}

	return json.Unmarshal([]byte(target), one)
}

func StringPtr(v string) *string {
	return &v
}
