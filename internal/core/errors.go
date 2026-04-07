package core

import "errors"

var (
	ErrInvalidConsumerConfig = errors.New("invalid consumer config")
	ErrDuplicateSubscription = errors.New("duplicate subscription")
)
