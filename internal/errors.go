package internal

import (
	"github.com/MarwanAlsoltany/serrors"
)

// NOTE: this file needs to exist here to avoid circular imports between the top-level package
// and internal sub-packages that all need to reference the same error domain and root sentinel.

// Error is a type alias so sub-packages can use [internal.Error] without importing serrors directly.
type Error = serrors.Error

// ErrDomain is the shared error domain for the entire broker package.
// All sub-packages call [ErrDomain.Sentinel] or [ErrDomain.Wrap] to define their sentinels,
// ensuring every broker error is reachable from [ErrBroker] via [errors.Is].
var ErrDomain = serrors.New("broker")

// ErrBroker is the root sentinel for all broker errors.
// errors.Is(err, ErrBroker) returns true for any *Error produced by [ErrDomain].
var ErrBroker = ErrDomain.Root()
