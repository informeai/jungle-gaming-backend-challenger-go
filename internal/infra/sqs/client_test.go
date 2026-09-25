package sqs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// responseError mirrors the SDK: a status of -1 means no response attached.
func responseError(status int, cause error) error {
	var resp *smithyhttp.Response
	if status >= 0 {
		resp = &smithyhttp.Response{Response: &http.Response{StatusCode: status}}
	}
	return fmt.Errorf("operation error SQS: SendMessage, %w", &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{Response: resp, Err: cause},
	})
}

func TestIsUnavailable(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil": {nil, false},
		"status 0 (timeout, as the SDK reports it)": {responseError(0, context.DeadlineExceeded), true},
		"no response attached":                      {responseError(-1, context.DeadlineExceeded), true},
		"server error":                              {responseError(503, errors.New("service unavailable")), true},
		"bad request":                               {responseError(400, errors.New("invalid parameter")), false},
		"access denied":                             {responseError(403, errors.New("access denied")), false},
		"network error":                             {errors.New("dial tcp: connection refused"), true},
		"caller canceled":                           {context.Canceled, false},
		"deadline without a response":               {fmt.Errorf("wrapped: %w", context.DeadlineExceeded), true},
	}
	for name, c := range cases {
		if got := IsUnavailable(c.err); got != c.want {
			t.Errorf("%s: IsUnavailable = %v, want %v", name, got, c.want)
		}
	}
}
