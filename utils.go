package broker

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

func hash(value interface{}) string {
	text := fmt.Sprintf("%#v", value)
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func cloneMap[T ~map[string]interface{}](src T) T {
	if src == nil {
		return nil
	}
	dst := make(T, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
