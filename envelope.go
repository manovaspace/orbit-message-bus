package messagebus

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrInvalidEnvelope is returned when the envelope fails validation.
	ErrInvalidEnvelope = errors.New("messagebus: invalid envelope")
	// ErrInvalidSubject is returned when the subject fails validation.
	ErrInvalidSubject = errors.New("messagebus: invalid subject")
)

// Envelope is the Orbit NATS message envelope (enforced at publish/subscribe).
type Envelope struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	Timestamp     time.Time         `json:"timestamp"`
	CorrelationID string            `json:"correlation_id"`
	TraceID       string            `json:"trace_id,omitempty"`
	Payload       json.RawMessage   `json:"payload"`
	Metadata      map[string]string `json:"metadata"`
}

// Validate checks required envelope fields.
func (e Envelope) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("%w: missing id", ErrInvalidEnvelope)
	}
	if _, err := uuid.Parse(e.ID); err != nil {
		return fmt.Errorf("%w: id must be uuid", ErrInvalidEnvelope)
	}
	if e.Type == "" {
		return fmt.Errorf("%w: missing type", ErrInvalidEnvelope)
	}
	if err := ValidateSubject(e.Type); err != nil {
		return fmt.Errorf("%w: type %v", ErrInvalidEnvelope, err)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: missing timestamp", ErrInvalidEnvelope)
	}
	if e.CorrelationID == "" {
		return fmt.Errorf("%w: missing correlation_id", ErrInvalidEnvelope)
	}
	if _, err := uuid.Parse(e.CorrelationID); err != nil {
		return fmt.Errorf("%w: correlation_id must be uuid", ErrInvalidEnvelope)
	}
	if e.Payload == nil {
		return fmt.Errorf("%w: missing payload", ErrInvalidEnvelope)
	}
	if !json.Valid(e.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidEnvelope)
	}
	if e.Metadata == nil {
		return fmt.Errorf("%w: missing metadata", ErrInvalidEnvelope)
	}
	return nil
}

// MarshalJSON encodes the envelope.
func (e Envelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// UnmarshalEnvelope decodes and validates JSON bytes.
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// NewEnvelope builds a valid envelope with generated ids.
func NewEnvelope(msgType string, payload json.RawMessage, source, version string) (Envelope, error) {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	e := Envelope{
		ID:            uuid.NewString(),
		Type:          msgType,
		Timestamp:     time.Now().UTC(),
		CorrelationID: uuid.NewString(),
		Payload:       payload,
		Metadata: map[string]string{
			"source":  source,
			"version": version,
		},
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}
