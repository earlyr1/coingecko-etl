package s3

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestIsPreconditionFailed(t *testing.T) {
	byCode := fmt.Errorf("wrapped: %w", &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold"})
	byStatus := &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 412}},
		Err:      errors.New("boom"),
	}}
	other := &smithy.GenericAPIError{Code: "AccessDenied"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"error code", byCode, true},
		{"http status", byStatus, true},
		{"other api error", other, false},
		{"plain error", errors.New("dial tcp: refused"), false},
	}
	for _, c := range cases {
		if got := isPreconditionFailed(c.err); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestIsBucketExists(t *testing.T) {
	if !isBucketExists(&smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou"}) {
		t.Error("BucketAlreadyOwnedByYou not recognised")
	}
	if !isBucketExists(&smithy.GenericAPIError{Code: "BucketAlreadyExists"}) {
		t.Error("BucketAlreadyExists not recognised")
	}
	if isBucketExists(&smithy.GenericAPIError{Code: "AccessDenied"}) {
		t.Error("AccessDenied treated as exists")
	}
}

func TestMetaValueCaseInsensitive(t *testing.T) {
	for _, k := range []string{"sha256", "Sha256", "SHA256"} {
		v, ok := metaValue(map[string]string{k: "abc"}, "sha256")
		if !ok || v != "abc" {
			t.Errorf("key %q: got %q %v", k, v, ok)
		}
	}
	if _, ok := metaValue(map[string]string{"other": "x"}, "sha256"); ok {
		t.Error("missing key reported present")
	}
}
