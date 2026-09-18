package sync

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Snapshot 是某一版本向量时刻的全量状态快照。
type Snapshot struct {
	Vector  VersionVector           `json:"vector"`
	Entries map[string]SnapshotEntry `json:"entries"`
}

// SnapshotEntry 记录单个 key 的当前值（或墓碑）及其元数据。
// 墓碑被保留在快照中：否则持有旧状态的副本在同步时无法得知删除发生。
type SnapshotEntry struct {
	Value   json.RawMessage `json:"value,omitempty"`
	Deleted bool            `json:"deleted"`
	HLC     HLC             `json:"hlc"`
}

// snapshotPath 返回引擎目录下的快照文件路径。
func (e *Engine) snapshotPath() string {
	return filepath.Join(filepath.Dir(e.store.path), "snapshot.json")
}

// loadSnapshot 在 Open 时加载快照（若存在）。快照文件原子写入，
// 因此要么完整要么不存在；损坏的临时文件会被忽略。
func (e *Engine) loadSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	e.vv = s.Vector.Clone()
	for k, se := range s.Entries {
		e.state[k] = entry{value: se.Value, deleted: se.Deleted, hlc: se.HLC}
	}
	return nil
}

// saveSnapshot 原子地写快照：临时文件 + fsync + rename。
func saveSnapshot(path string, s Snapshot) error {
	tmp := path + ".tmp"
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Compact 基于当前状态生成快照并压缩变更日志。
//
// 与并发变更的交互：先在锁内固定快照版本向量与状态，再在锁外写快照文件，
// 最后重新取锁，仅丢弃被快照向量覆盖的变更——压缩期间新产生的变更
// 不被覆盖，因此既不会丢失也不会重复应用。
// 未知类型的已覆盖变更仍被保留在日志中，以便未来版本升级后能够合并。
func (e *Engine) Compact() error {
	e.mu.RLock()
	snap := Snapshot{
		Vector:  e.vv.Clone(),
		Entries: make(map[string]SnapshotEntry, len(e.state)),
	}
	for k, en := range e.state {
		snap.Entries[k] = SnapshotEntry{Value: en.value, Deleted: en.deleted, HLC: en.hlc}
	}
	e.mu.RUnlock()

	if err := saveSnapshot(e.snapshotPath(), snap); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	var remaining []Change
	for _, c := range e.changes {
		covered := snap.Vector.Has(c.ID)
		known := c.Type == ChangeSet || c.Type == ChangeDel
		if !covered || !known {
			remaining = append(remaining, c)
		}
	}
	if err := e.store.Replace(remaining); err != nil {
		return err
	}
	e.changes = remaining
	return nil
}
