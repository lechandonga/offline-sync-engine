package sync

// Snapshot 是某一版本向量时刻的全量状态快照。
import "encoding/json"
type Snapshot struct {
	Vector VersionVector                `json:"vector"`
	Entries map[string]SnapshotEntry   `json:"entries"`
}

// SnapshotEntry 记录单个 key 的当前值（或墓碑）及其元数据。
type SnapshotEntry struct {
	Value   json.RawMessage `json:"value,omitempty"`
	Deleted bool            `json:"deleted"`
	HLC     HLC             `json:"hlc"`
}

// Compact 基于当前状态生成快照并压缩变更日志（占位实现）。
// 压缩期间产生的新变更不会被丢失或重复应用。
func (e *Engine) Compact() error { return nil }
