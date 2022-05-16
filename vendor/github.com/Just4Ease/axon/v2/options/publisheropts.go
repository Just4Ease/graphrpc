package options

import (
	"context"
	"errors"
	"strings"
)

type PublisherOptions struct {
	contentType        string
	ctx                context.Context
	headers            map[string]string
	version            string
	disablePersistence bool
}

type PublisherOption func(o *PublisherOptions) error

func SetPubContentType(ContentType string) PublisherOption {
	return func(o *PublisherOptions) error {
		o.contentType = ContentType
		return nil
	}
}

func SetPubContext(ctx context.Context) PublisherOption {
	return func(o *PublisherOptions) error {
		if ctx == nil {
			return errors.New("invalid context")
		}
		o.ctx = ctx
		return nil
	}
}

func SetPubHeader(key, value string) PublisherOption {
	return func(o *PublisherOptions) error {
		if o.headers == nil {
			o.headers = make(map[string]string)
		}

		o.headers[key] = value
		return nil
	}
}

func DisablePubStreaming() PublisherOption {
	return func(o *PublisherOptions) error {
		o.disablePersistence = true
		return nil
	}
}

func SetPubHeaders(headers map[string]string) PublisherOption {
	return func(o *PublisherOptions) error {
		if o.headers == nil {
			o.headers = headers
			return nil
		}

		for k, v := range headers {
			o.headers[k] = v
		}
		return nil
	}
}

func SetPubMsgVersion(version string) PublisherOption {
	return func(o *PublisherOptions) error {
		if strings.TrimSpace(version) == "" {
			return nil
		}

		o.version = version
		return nil
	}
}

func (p *PublisherOptions) Context() context.Context {
	return p.ctx
}

func (p *PublisherOptions) ContentType() string {
	return p.contentType
}

func (p *PublisherOptions) Headers() map[string]string {
	return p.headers
}

func (p *PublisherOptions) SpecVersion() string {
	return p.version
}

func (p *PublisherOptions) IsStreamingDisabled() bool {
	return p.disablePersistence
}

func DefaultPublisherOptions(opts ...PublisherOption) (*PublisherOptions, error) {
	p := &PublisherOptions{
		ctx:                context.Background(),
		contentType:        "application/json",
		version:            "default",
		headers:            make(map[string]string),
		disablePersistence: false,
	}

	for _, o := range opts {
		if err := o(p); err != nil {
			return nil, err
		}
	}

	return p, nil
}
