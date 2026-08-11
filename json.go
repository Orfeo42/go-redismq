package go_redismq

import (
	"encoding/json"
	"errors"
)

var ErrJsonTargetNil = errors.New("redismq: target is nil")

func FormatToJsonString(target any) string {
	if target == nil {
		return ""
	}

	encodeString, err := json.Marshal(target)
	if err != nil {
		return ""
	}

	return string(encodeString)
}

func MarshalToJsonString(target any) string {
	if target == nil {
		return ""
	}

	marshal, err := json.Marshal(target)
	if err != nil {
		return ""
	}

	return string(marshal)
}

func MarshalMetadataToJsonString(target any) *string {
	if target == nil {
		return nil
	}

	marshal, err := json.Marshal(target)
	if err != nil {
		return nil
	}

	return String(string(marshal))
}

func UnmarshalFromJsonString(target string, one any) error {
	if len(target) == 0 {
		return ErrJsonTargetNil
	}

	return json.Unmarshal([]byte(target), one)
}

// String returns a pointer to the string value passed in.
func String(v string) *string {
	return &v
}
