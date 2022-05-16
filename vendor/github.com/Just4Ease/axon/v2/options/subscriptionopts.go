package options

import (
	"context"
	"errors"
	"strings"
)

// SubscriptionType of subscription supported by most messaging systems. ( Pulsar,
type SubscriptionType int

const (
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = iota

	// Shared subscription mode, multiple consumer will be able to use the same subscription name
	// and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared

	// Failover subscription mode, multiple consumer will be able to use the same subscription name
	// but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	// KeyShared subscription mode, multiple consumer will be able to use the same
	// subscription and all messages with the same key will be dispatched to only one consumer
	KeyShared
)

func (t SubscriptionType) IsValid() bool {
	switch t {
	case Exclusive, Shared, Failover, KeyShared:
		return true
	default:
		return false
	}
}

func (t SubscriptionType) String() string {
	return []string{"Exclusive", "Shared", "Failover", "KeyShared"}[t]
}

type SubscriptionOptions struct {
	subscriptionType   SubscriptionType
	disableStreaming   bool
	contentType        string
	ctx                context.Context
	messageSpecVersion string
	maxReDelivery      int
}

type SubscriptionOption func(o *SubscriptionOptions) error

func SetSubContentType(contentType string) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.contentType = contentType
		return nil
	}
}

func SetExpectedMessageSpecVersion(version string) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		if strings.TrimSpace(version) == "" {
			return errors.New("invalid message version")
		}
		o.messageSpecVersion = version
		return nil
	}
}

func SetSubContext(ctx context.Context) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		if ctx == nil {
			return errors.New("invalid context")
		}

		o.ctx = ctx
		return nil
	}
}

func SetSubType(t SubscriptionType) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		if !t.IsValid() {
			return errors.New("invalid subscription type")
		}

		o.subscriptionType = t
		return nil
	}
}

func SetSubMaxRedelivery(n int) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.maxReDelivery = n
		return nil
	}
}

func DisableSubStreaming() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.disableStreaming = true
		return nil
	}
}

func DefaultSubOptions(opts ...SubscriptionOption) (*SubscriptionOptions, error) {
	s := &SubscriptionOptions{
		ctx:                context.Background(),
		subscriptionType:   Shared,
		maxReDelivery:      5,
		disableStreaming:   false,
		messageSpecVersion: "default",
		contentType:        "application/json",
	}
	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s SubscriptionOptions) Context() context.Context {
	return s.ctx
}

func (s SubscriptionOptions) ContentType() string {
	return s.contentType
}

func (s SubscriptionOptions) SubscriptionType() SubscriptionType {
	return s.subscriptionType
}

func (s SubscriptionOptions) MaxRedelivery() int {
	return s.maxReDelivery
}

func (s SubscriptionOptions) IsStreamingDisabled() bool {
	return s.disableStreaming
}

func (s SubscriptionOptions) ExpectedSpecVersion() string {
	return s.messageSpecVersion
}
