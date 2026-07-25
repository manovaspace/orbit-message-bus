package messagebus

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultMaxAttempts is the default retry cap for message handlers.
const DefaultMaxAttempts = 3

// Handler processes a validated envelope.
type Handler func(ctx context.Context, env Envelope) error

// Bus is the Orbit NATS message bus port.
type Bus interface {
	Publish(ctx context.Context, subject string, env Envelope) error
	Subscribe(ctx context.Context, subject string, handler Handler) error
	Request(ctx context.Context, subject string, env Envelope) (*Envelope, error)
	Close() error
}

// Config holds NATS connection options.
type Config struct {
	URL          string
	MaxAttempts  int
	Source       string
	Version      string
}

func (c *Config) attempts() int {
	if c.MaxAttempts <= 0 {
		return DefaultMaxAttempts
	}
	return c.MaxAttempts
}

// MaxAttemptsOrDefault returns configured max attempts or DefaultMaxAttempts.
func (c Config) MaxAttemptsOrDefault() int {
	return (&c).attempts()
}

// DomainFromSubject returns the domain segment (first part) of a subject.
func DomainFromSubject(subject string) (string, error) {
	parts := splitSubject(subject)
	if len(parts) != 3 {
		return "", fmt.Errorf("messagebus: subject %q must be {domain}.{entity}.{action}", subject)
	}
	return parts[0], nil
}

// DLQSubject returns the dead-letter subject for a primary subject.
func DLQSubject(subject string) string {
	return subject + ".dlq"
}

// ValidateSubject checks {domain}.{entity}.{action} shape (three dot-separated parts).
func ValidateSubject(subject string) error {
	parts := splitSubject(subject)
	if len(parts) != 3 {
		return fmt.Errorf("messagebus: subject %q must be {domain}.{entity}.{action}", subject)
	}
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("messagebus: subject %q has empty segment", subject)
		}
	}
	return nil
}

func splitSubject(subject string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == '.' {
			parts = append(parts, subject[start:i])
			start = i + 1
		}
	}
	parts = append(parts, subject[start:])
	return parts
}

// RetryDelay returns exponential backoff with jitter for attempt n (1-based).
func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
	if base > 5*time.Second {
		base = 5 * time.Second
	}
	// ponytail: jitter via attempt hash — upgrade to crypto/rand if herd matters
	jitter := time.Duration(attempt*37%50) * time.Millisecond
	return base + jitter
}

// IsValidationError marks errors that must not be retried.
func IsValidationError(err error) bool {
	return errors.Is(err, ErrInvalidEnvelope) || errors.Is(err, ErrInvalidSubject)
}
