package sync

import "sync/atomic"

// HLC 是混合逻辑时钟值：物理毫秒时间 + 逻辑计数器 + 副本 ID 作为最终决胜项。
// 比较顺序为 (Millis, Counter, Replica)，全序且与节点身份无关地确定
// （同一事件在任何副本上比较结果一致；并发事件按固定规则决胜）。
type HLC struct {
	Millis  int64  `json:"ms"`
	Counter uint32 `json:"ctr"`
	Replica string `json:"r"`
}

// Compare 返回 -1/0/1，定义全局确定性全序。
func (h HLC) Compare(o HLC) int {
	if h.Millis != o.Millis {
		if h.Millis < o.Millis {
			return -1
		}
		return 1
	}
	if h.Counter != o.Counter {
		if h.Counter < o.Counter {
			return -1
		}
		return 1
	}
	switch {
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
	last    atomic.Int64 // 高 32 位存 millis 低位简化：直接存上次 millis
	ctr     atomic.Uint32
}

// NewClock 创建时钟。
func NewClock(replica string) *Clock {
	return &Clock{replica: replica}
}

// Now 生成一个新的 HLC 值（占位实现，后续填充）。
func (c *Clock) Now() HLC { return HLC{Replica: c.replica} }

// Observe 在收到远端 HLC 后推进本地时钟（占位实现，后续填充）。
func (c *Clock) Observe(remote HLC) {}
