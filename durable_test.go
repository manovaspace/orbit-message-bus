package messagebus

import "testing"

func TestDurableNameSanitizesDots(t *testing.T) {
	got := durableName("auth.otp.requested")
	want := "orbit-auth_otp_requested"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
