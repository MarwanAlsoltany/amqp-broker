package endpoint

import (
	"sync"
)

// noCopy may be embedded into structs which must not be copied after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527 for details.
type noCopy struct{}

var _ sync.Locker = (*noCopy)(nil)

// Lock is a no-op used by -copylocks checker from "go vet".
func (*noCopy) Lock() {}

// Unlock is a no-op used by -copylocks checker from "go vet".
func (*noCopy) Unlock() {}
