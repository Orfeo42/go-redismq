package idgen

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/google/uuid"
)

func RandomAlphanumeric(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

func UniqueNo(districtCode string) string {
	id := uuid.New()

	return fmt.Sprintf("MQCT_%s_%s", districtCode, strings.ReplaceAll(id.String(), "-", ""))
}
