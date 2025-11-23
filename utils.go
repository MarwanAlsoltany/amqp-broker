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
