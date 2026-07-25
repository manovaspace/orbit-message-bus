package messagebus_test

import (
	"encoding/json"
	"testing"
	"time"

	messagebus "github.com/manovaspace/orbit-message-bus"
)

func TestEnvelopeValidate(t *testing.T) {
	env, err := messagebus.NewEnvelope("auth.otp.requested", json.RawMessage(`{"email":"a@b.c"}`), "orbit-auth", "1")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEnvelopeRejectsBadSubject(t *testing.T) {
	_, err := messagebus.NewEnvelope("auth.otp", json.RawMessage(`{}`), "orbit-auth", "1")
	if err == nil {
		t.Fatal("expected error for two-part subject")
	}
}

func TestValidateSubject(t *testing.T) {
	if err := messagebus.ValidateSubject("auth.otp.requested"); err != nil {
		t.Fatal(err)
	}
	if err := messagebus.ValidateSubject("bad"); err == nil {
		t.Fatal("expected error")
	}
}

func TestDLQSubject(t *testing.T) {
	got := messagebus.DLQSubject("auth.otp.requested")
	if got != "auth.otp.requested.dlq" {
		t.Fatalf("got %q", got)
	}
}

func TestRetryDelayBounded(t *testing.T) {
	d := messagebus.RetryDelay(10)
	if d > 6*time.Second {
		t.Fatalf("delay too large: %v", d)
	}
}
