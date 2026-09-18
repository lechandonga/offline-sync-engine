package sync

import (
	"sync"
	"time"
)

// HLC 是混合逻辑时钟值：物理毫秒时间 + 逻辑计数器 + 副本 ID 作为最终决胜项。
// 比较顺序为 (Millis, Counter, Replica)，在任何副本上比较结果一致，
// 因此并发冲突的解决不依赖消息到达顺序；Replica 仅作为同毫秒同计数器时
// 的确定性决胜项，不赋予任何节点优先级语义之外的歧义。
type HLC struct {
	Millis  int64  `json:"ms"`
	Counter uint32 `json:"ctr"`
	Replica string `json:"r"`
}

// Compare 返回 -1/0/1，定义全局确定性全序。
func (h HLC) Compare(o HLC) int {
	switch {
	case h.Millis < o.Millis:
		return -1
	case h.Millis > o.Millis:
		return 1
	case h.Counter < o.Counter:
		return -1
	case h.Counter > o.Counter:
		return 1
	case h.Replica < o.Replica:
		return -1
	case h.Replica > o.Replica:
		return 1
	default:
		return 0
	}
}

// Clock 为单个副本生成单调递增的 HLC 值。
type Clock struct {
	replica string
	mu      sync.Mutex
	millis  int64
	counter uint32
}

// NewClock 创建时钟。
func NewClock(replica string) *Clock {
	return &Clock{replica: replica}
}

// Now 生成一个新的、严格大于此前所有本地值的 HLC。
func (c *Clock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixMilli()
	if now <= c.millis {
		// 物理时钟未前进（或回退）：递增逻辑计数器，保证单调。
		c.counter++
	} else {
		c.millis = now
		c.counter = 0
	}
	return HLC{Millis: c.millis, Counter: c.counter, Replica: c.replica}
}

// Observe 在收到远端 HLC 后推进本地时钟，
// 保证后续本地事件的 HLC 大于已观察到的任何事件。
func (c *Clock) Observe(remote HLC) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixMilli()
	switch {
	case remote.Millis > c.millis && remote.Millis > now:
		c.millis = remote.Millis
		c.counter = remote.Counter + 1
	case remote.Millis == c.millis:
		if remote.Counter >= c.counter {
			c.counter = remote.Counter + 1
		}
	default:
		if now > c.millis {
			c.millis = now
			c.counter = 0
		} else {
			c.counter++
		}
	}
}
