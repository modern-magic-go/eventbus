package eventbus

import "errors"

var (
	// ErrInvalidConsumerConfig 表示订阅配置不合法。
	ErrInvalidConsumerConfig = errors.New("invalid consumer config")
	// ErrDuplicateSubscription 表示同 topic/group 已存在订阅。
	ErrDuplicateSubscription = errors.New("duplicate subscription")
)

