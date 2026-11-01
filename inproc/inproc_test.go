package inproc_test

import (
	"testing"

	messagebus "github.com/manovaspace/orbit-message-bus"
	"github.com/manovaspace/orbit-message-bus/bustest"
	"github.com/manovaspace/orbit-message-bus/inproc"
)

func TestConformance(t *testing.T) {
	bustest.Conformance(t, func(_ *testing.T) messagebus.Bus {
		return inproc.New(messagebus.Config{Source: "conformance", Version: "1"})
	})
}
