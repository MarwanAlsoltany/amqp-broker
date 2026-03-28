// Package internal provides the broker-scoped error domain and root sentinel error.
// All broker sub-packages use [ErrDomain] to declare their error sentinels,
// ensuring every broker error is reachable from [ErrBroker] via [errors.Is].
package internal
