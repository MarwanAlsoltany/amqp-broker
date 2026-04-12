package topology

import (
	"strings"
)

var (
	// ErrTopologyRoutingKeyEmpty indicates a routing key is empty.
	// Some operations require non-empty routing keys.
	ErrTopologyRoutingKeyEmpty = ErrTopologyValidation.Derive("routing key empty")
)

// RoutingKey represents an AMQP routing key with placeholder substitution.
// Placeholders like {key} are replaced with values from a map.
type RoutingKey string

// NewRoutingKey creates a RoutingKey with placeholder substitution support.
// Placeholders in the format {name} are replaced with values from the map.
//
// Example:
//
//	// template: "orders.{region}.{action}"
//	rk := NewRoutingKey("orders.{region}.{action}", map[string]string{
//		"region": "us-east",
//		"action": "created",
//	})
//	// result: "orders.us-east.created"
func NewRoutingKey(key string, placeholders map[string]string) RoutingKey {
	rk := RoutingKey(key)
	rk.Replace(placeholders)
	return rk
}

// Replace replaces placeholders in the routing key with values from the provided map.
// Placeholders are in the format {key}. For example, given a routing key "user.{id}.update"
// and placeholders map {"id": "123"}, their Replace method will produce "user.123.update".
func (rk *RoutingKey) Replace(placeholders map[string]string) {
	// replace {} placeholders with values from args
	pattern := string(*rk)
	for k, v := range placeholders {
		placeholder := "{" + k + "}"
		pattern = strings.ReplaceAll(pattern, placeholder, v)
	}
	*rk = RoutingKey(pattern)
}

// Validate returns an error if the routing key is invalid.
func (rk *RoutingKey) Validate() error {
	if rk != nil && string(*rk) == "" {
		return ErrTopologyRoutingKeyEmpty
	}
	return nil
}

// String returns the string representation of the routing key.
func (rk RoutingKey) String() string {
	return string(rk)
}
