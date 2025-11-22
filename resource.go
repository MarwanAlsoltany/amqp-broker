package broker

// resourceWrapper wraps a pooled resource with its release function.
type resourceWrapper[T any] struct {
	resource *T
	release  releaseFunc
}

// releaseFunc is called to return a resource to its pool.
// The bad parameter indicates whether the resource is faulty and should be discarded.
type releaseFunc func(bad bool)
