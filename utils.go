package broker

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
)

// hash computes an MD5 hash of the given value. The value is encoded using gob before hashing.
// If the value is nil, it hashes the string representation of nil.
// Returns the hash as a hexadecimal string.
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

// contextWithAnyCancel returns a derived context and cancel func.
// The derived context is cancelled when either:
//   - The returned cancel() is called, or
//   - The parent context is cancelled, or
//   - The other context is cancelled
func contextWithAnyCancel(parent, other context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)

	if other == nil {
		return ctx, cancel
	}

	// watch for other cancelation, exit when either other or ctx is done
	go func() {
		select {
		case <-other.Done():
			// if other cancels, cancel our derived ctx
			cancel()
		case <-ctx.Done():
			// no-op: ctx already cancelled (either by caller or parent)
		}
	}()

	return ctx, cancel
}
