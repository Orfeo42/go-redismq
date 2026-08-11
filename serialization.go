package go_redismq

import (
	"reflect"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/errors/gerror"
)

func Serialize(target interface{}) []byte {
	jsonData, _ := gjson.Marshal(target)

	return jsonData
}

func Deserialize(body []byte, v interface{}) (err error) {
	if !isPointerType(v) {
		err = gerror.New("v should be pointer type")

		return
	}

	err = gjson.Unmarshal(body, &v) // Unmarshal todo mark 加上 &

	return
}

func isPointerType(value interface{}) bool {
	typ := reflect.TypeOf(value)
	kind := typ.Kind()

	return kind == reflect.Pointer
}

//func Deserialize(body []byte) interface{} {
//	var result interface{}
//	err := gjson.Unmarshal(body, &result)
//	if err != nil {
//		fmt.Printf("Deserialize err:%s\n", err)
//	}
//	return result
//}

type Person struct {
	Name   string
	Age    int
	Emails []string
}
