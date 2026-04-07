package eventbus

import "encoding/json"

// Codec 定义 payload 编解码能力。
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (c jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

