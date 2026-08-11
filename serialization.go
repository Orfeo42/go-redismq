package go_redismq

import (
	"encoding/json"
	"errors"
	"reflect"
)

var ErrDeserializeTargetNotPointer = errors.New("redismq: v should be pointer type")

func Serialize(target any) []byte {
	jsonData, err := json.Marshal(target)
	if err != nil {
		return nil
	}

	return jsonData
}

func Deserialize(body []byte, v any) error {
	if !isPointerType(v) {
		return ErrDeserializeTargetNotPointer
	}

	return json.Unmarshal(body, v)
}

func isPointerType(value any) bool {
	typ := reflect.TypeOf(value)
	kind := typ.Kind()

	return kind == reflect.Pointer
}

type Person struct {
	Name   string
	Age    int
	Emails []string
}
