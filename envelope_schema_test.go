package messagebus_test

import (
	"path/filepath"
	"testing"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/bustest"
)

func TestEnvelopeSchema(t *testing.T) {
	bustest.CheckEventSchema(t, messagebus.Envelope{}, filepath.Join("testdata", "envelope.schema.json"))
}
