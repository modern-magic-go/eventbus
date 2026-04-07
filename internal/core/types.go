package core

import (
	"context"
	"time"
)

const (
	DefaultMaxRetries     = 3
	DefaultPollBatchSize  = 10
	DefaultPollBlock      = 2 * time.Second
	DefaultRetryBaseDelay = 5 * time.Second
)

type Message struct {
	ID            string
	Topic         string
	Payload       []byte
	Headers       map[string]string
	PublishedAt   time.Time
	ConsumerGroup string
	Attempt       int
	MaxRetries    int
	DeliveryTag   string
}

type Handler func(ctx context.Context, msg Message) error

type ConsumerConfig struct {
	Topic          string
	Group          string
	ConsumerName   string
	Handler        Handler
	MaxRetries     int
	PollBatchSize  int
	PollBlock      time.Duration
	RetryBaseDelay time.Duration
}

type RetryTask struct {
	Topic       string            `json:"topic"`
	Group       string            `json:"group"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempt     int               `json:"attempt"`
	MaxRetries  int               `json:"max_retries"`
	AvailableAt time.Time         `json:"available_at"`
	LastError   string            `json:"last_error,omitempty"`

	rawMember string
}

type DeadLetterMessage struct {
	Topic      string            `json:"topic"`
	Group      string            `json:"group"`
	Payload    []byte            `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
	Attempt    int               `json:"attempt"`
	MaxRetries int               `json:"max_retries"`
	Error      string            `json:"error"`
	OccurredAt time.Time         `json:"occurred_at"`
}

type PublishOptions struct {
	MessageID string
	Headers   map[string]string
}

type PublishOption func(*PublishOptions)

func WithMessageID(id string) PublishOption {
	return func(o *PublishOptions) {
		o.MessageID = id
	}
}

func WithHeaders(headers map[string]string) PublishOption {
	return func(o *PublishOptions) {
		o.Headers = headers
	}
}

func normalizeConsumerConfig(cfg ConsumerConfig) ConsumerConfig {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.PollBatchSize <= 0 {
		cfg.PollBatchSize = DefaultPollBatchSize
	}
	if cfg.PollBlock <= 0 {
		cfg.PollBlock = DefaultPollBlock
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = DefaultRetryBaseDelay
	}
	return cfg
}
