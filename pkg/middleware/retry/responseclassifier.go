package retry

import (
	"github.com/prizem-io/h2/proxy"
)

// ResponseType denotes that a request is a success, retryable failure, or non-retryable failure.
type ResponseType int

const (
	// Success indicates a successful HTTP request.
	Success ResponseType = iota
	// RetryableFailure indicates a failed HTTP request that may be safely retried.
	RetryableFailure
	// Failure indicates a failed HTTP request that may NOT be safely retried.
	Failure
)

type (
	// ResponseClassifier examines the HTTP method and response status code and determines
	// if the request was successful, a failure that is considered retryable, or a non-retryable
	// failure.
	// Inspired by Linkerd 1.X
	ResponseClassifier func(requestHeaders proxy.Headers, status int) ResponseType

	// ClassifierOption defines options for `NewResponseClassifier`.
	ClassifierOption func(*classfierOpts)

	// RetryableTest is a function that determines if a failure is retryable.
	RetryableTest func(requestHeaders proxy.Headers, status int) bool

	classfierOpts struct {
		retryableMethods map[string]struct{}
		retryableTests   []RetryableTest
	}
)

// ResponseClassifiers is a registry of available response classifiers.
var ResponseClassifiers = map[string]ResponseClassifier{
	"nonRetryable5XX":          NonRetryable5XX,
	"retryableRead5XX":         RetryableRead5XX,
	"retryableIdempotent5XX":   RetryableIdempotent5XX,
	"retryableAll5XX":          RetryableAll5XX,
	"allSuccessful":            AllSuccessful,
	"retryableIdempotencyKeys": RetryableIdempotencyKeys,
}

// WithRetryableMethods specifies which methods are retryable on a 5XX status code.
func WithRetryableMethods(methods ...string) ClassifierOption {
	return func(o *classfierOpts) {
		if o.retryableMethods == nil {
			o.retryableMethods = make(map[string]struct{}, len(methods))
		}
		for _, method := range methods {
			o.retryableMethods[method] = struct{}{}
		}
	}
}

// WithRetryableTests specifies test functions to execute that determine if a failure is retryable.
func WithRetryableTests(tests ...RetryableTest) ClassifierOption {
	return func(o *classfierOpts) {
		o.retryableTests = append(o.retryableTests, tests...)
	}
}

// NewResponseClassifier creates a `ResponseClassifier` using various classifier options.
func NewResponseClassifier(opts ...ClassifierOption) ResponseClassifier {
	var o classfierOpts
	for _, opt := range opts {
		opt(&o)
	}

	return func(requestHeaders proxy.Headers, status int) ResponseType {
		failure := status/100 == 5

		if failure {
			if o.retryableMethods != nil {
				method := requestHeaders.ByName(":method")
				if _, ok := o.retryableMethods[method]; ok {
					return RetryableFailure
				}
			}
			if o.retryableTests != nil {
				for _, test := range o.retryableTests {
					if test(requestHeaders, status) {
						return RetryableFailure
					}
				}
			}
			if o.retryableMethods != nil || o.retryableTests != nil {
				return Failure
			}
		}

		return Success
	}
}

// HasOneOfHeaders checks for the presence of header names that match any element of `names`.
func HasOneOfHeaders(names ...string) RetryableTest {
	return func(requestHeaders proxy.Headers, status int) bool {
		for _, name := range names {
			if requestHeaders.ByName(name) != "" {
				return true
			}
		}
		return false
	}
}

// HasIdempotencyKeyHeader checks for the presence of the `x-idempotency-key` header.
var HasIdempotencyKeyHeader = HasOneOfHeaders("x-idempotency-key")

// NonRetryable5XX considers all 5XX responses to be failures and not retryable.
var NonRetryable5XX = NewResponseClassifier(
	WithRetryableMethods())

// RetryableRead5XX considers all 5XX responses to be failures.
// However, GET, HEAD, OPTIONS, and TRACE requests may be retried.
var RetryableRead5XX = NewResponseClassifier(
	WithRetryableMethods("GET", "HEAD", "OPTIONS", "TRACE"))

// RetryableIdempotent5XX is like RetryableRead5XX but PUT and DELETE requests may also be retried.
var RetryableIdempotent5XX = NewResponseClassifier(
	WithRetryableMethods("GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"))

// RetryableAll5XX is like RetryableIdempotent5XX but POST and PATCH requests may also be retried.
var RetryableAll5XX = NewResponseClassifier(
	WithRetryableMethods("GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE", "POST", "PATCH"))

// AllSuccessful considers all responses to be successful, regardless of status code.
var AllSuccessful = NewResponseClassifier()

// RetryableIdempotencyKeys is like RetryableRead5XX but any write method is retryable if an idempotency key header is provided.
var RetryableIdempotencyKeys = NewResponseClassifier(
	WithRetryableMethods("GET", "HEAD", "OPTIONS", "TRACE"),
	WithRetryableTests(HasIdempotencyKeyHeader))
