package broker

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
)

func hash(value interface{}) string {
	var buffer bytes.Buffer

	if value == nil {
		fmt.Fprintf(&buffer, "%#v", value)
	} else {
		gob.NewEncoder(&buffer).Encode(value)
	}

	hash := md5.Sum(buffer.Bytes())
	return hex.EncodeToString(hash[:])
}
