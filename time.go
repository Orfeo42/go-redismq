package redismq

import "time"

func currentTimeMillis() (s int64) {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}

	return b
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}

	return b
}
