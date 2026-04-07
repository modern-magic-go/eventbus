package snowflake

import (
	"crypto/rand"
	"errors"
	"os"
	"sync"
	"time"
)

const (
	epoch          = 1704067200000 // 2024-01-01 00:00:00 UTC in milliseconds
	timestampBits  = 41
	nodeBits       = 10
	sequenceBits   = 12
	maxSequence    = -1 ^ (-1 << sequenceBits)
	timestampShift = sequenceBits + nodeBits
	nodeShift      = sequenceBits
)

var (
	ErrClockBackward = errors.New("clock moved backward")
	globalGenerator  *Generator
	globalOnce       sync.Once
)

type Generator struct {
	mu       sync.Mutex
	nodeID   int64
	sequence int64
	lastTime int64
}

func NewGenerator(nodeID int64) (*Generator, error) {
	if nodeID < 0 || nodeID > (-1^(-1<<nodeBits)) {
		return nil, errors.New("nodeID out of range")
	}
	return &Generator{nodeID: nodeID, lastTime: -1}, nil
}

func (g *Generator) Generate() (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli() - epoch

	if now < g.lastTime {
		return 0, ErrClockBackward
	}

	if now == g.lastTime {
		g.sequence = (g.sequence + 1) & maxSequence
		if g.sequence == 0 {
			for now <= g.lastTime {
				now = time.Now().UnixMilli() - epoch
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTime = now
	return (now << timestampShift) | (g.nodeID << nodeShift) | g.sequence, nil
}

func (g *Generator) GenerateString() (string, error) {
	id, err := g.Generate()
	if err != nil {
		return "", err
	}
	return formatInt64(id), nil
}

func Generate() string {
	globalOnce.Do(func() {
		nodeID := generateNodeID()
		globalGenerator, _ = NewGenerator(nodeID)
	})
	id, _ := globalGenerator.GenerateString()
	return id
}

func generateNodeID() int64 {
	pid := int64(os.Getpid())
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return pid & 0x3FF
	}
	randPart := int64(b[0])<<8 | int64(b[1])
	return (pid ^ randPart) & 0x3FF
}

func formatInt64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	negative := n < 0
	if negative {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte(n%10) + '0'
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
