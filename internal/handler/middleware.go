package handler

var (
	// ErrMiddleware is the base error for handler middleware operations.
	// All errors returned by middlewares wrap this error for consistent error handling.
	ErrMiddleware = ErrHandler.Derive("middleware")
)

// Log field constants for structured logging attributes.
// Exposed as a map to allow runtime customization if needed.
var logFields = map[string]string{
	"panic":       "panic",
	"error":       "error",
	"action":      "action",
	"duration":    "duration",
	"id":          "id",
	"headers":     "headers",
	"body":        "body",
	"contentType": "contentType",
}

// noopMiddleware returns a no-op middleware that passes messages through unchanged.
func noopMiddleware() Middleware {
	return func(next Handler) Handler { return next }
}
