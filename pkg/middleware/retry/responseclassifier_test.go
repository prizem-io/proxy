package retry_test

import (
	"testing"

	"github.com/prizem-io/h2/proxy"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/http2/hpack"

	"github.com/prizem-io/proxy/pkg/middleware/retry"
)

func TestResponseClassifiers(t *testing.T) {
	type test struct {
		name       string
		headers    proxy.Headers
		classifier retry.ResponseClassifier
		expected   retry.ResponseType
	}
	tests := []test{}

	appendMethodTests := func(name string, classifier retry.ResponseClassifier, retryableMethods []string, nonRetryableMethods []string) {
		for _, method := range retryableMethods {
			tests = append(tests, test{
				name + " - " + method,
				proxy.Headers{methodHeader(method)},
				classifier,
				retry.RetryableFailure,
			})
		}
		for _, method := range nonRetryableMethods {
			tests = append(tests, test{
				name + " - " + method,
				proxy.Headers{methodHeader(method)},
				classifier,
				retry.Failure,
			})
		}
	}

	appendMethodTests("NonRetryable5XX", retry.NonRetryable5XX,
		methods(),
		methods("GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE", "POST", "PATCH"))
	appendMethodTests("RetryableRead5XX", retry.RetryableRead5XX,
		methods("GET", "HEAD", "OPTIONS", "TRACE"),
		methods("PUT", "DELETE", "POST", "PATCH"))
	appendMethodTests("RetryableIdempotent5XX", retry.RetryableIdempotent5XX,
		methods("GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"),
		methods("POST", "PATCH"))
	appendMethodTests("RetryableAll5XX", retry.RetryableAll5XX,
		methods("GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE", "POST", "PATCH"),
		methods())
	appendMethodTests("RetryableIdempotencyKeys", retry.RetryableIdempotencyKeys,
		methods("GET", "HEAD", "OPTIONS", "TRACE"),
		methods("PUT", "DELETE", "POST", "PATCH"))

	for _, method := range methods("GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE", "POST", "PATCH") {
		tests = append(tests, test{
			"AllSuccessful - " + method,
			proxy.Headers{methodHeader(method)},
			retry.AllSuccessful,
			retry.Success,
		})
	}

	tests = append(tests, []test{
		// RetryableIdempotencyKeys
		{
			"RetryableIdempotencyKeys - no idempotency key",
			proxy.Headers{methodHeader("POST")},
			retry.RetryableIdempotencyKeys,
			retry.Failure,
		},
		{
			"RetryableIdempotencyKeys - has idempotency key",
			proxy.Headers{
				methodHeader("POST"),
				{
					Name:  "x-idempotency-key",
					Value: "12345",
				},
			},
			retry.RetryableIdempotencyKeys,
			retry.RetryableFailure,
		},
	}...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.classifier(tt.headers, 500))
			assert.Equal(t, retry.Success, tt.classifier(tt.headers, 200))
		})
	}
}

func methodHeader(value string) hpack.HeaderField {
	return hpack.HeaderField{
		Name:  ":method",
		Value: value,
	}
}

func methods(values ...string) []string {
	return values
}
