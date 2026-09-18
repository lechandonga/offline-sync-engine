// Package sync 提供本地优先（local-first）的多副本数据同步能力：
// 离线编辑、增量同步、确定性冲突收敛、快照压缩与崩溃恢复。
package sync

import (
	"encoding/json"
	"fmt"
	"sort"
)

// ChangeType 描述变更类型。未知类型会被保留并继续复制，但不应用于本地状态，
// 以便旧版本副本与将来引入新类型的新版本副本互通。
type ChangeType string

const (
	// ChangeSet 表示对某个 key 的写入（新增或更新）。
	ChangeSet ChangeType = "set"
	// ChangeDel 表示对某个 key 的删除（写入墓碑）。
	ChangeDel ChangeType = "del"
)

// knownFields 是 Change 结构体已知的 JSON 字段集合，
// 用于在解码时捕获并保留未知字段（前向兼容）。
var knownFields = map[string]bool{
	"id": true, "replica": true, "seq": true, "type": true,
	"key": true, "value": true, "hlc": true,
}

// ChangeID 全局唯一标识一条变更，由产生它的副本 ID 与该副本内的单调序号组成。
type ChangeID struct {
	Replica string `json:"replica"`
	Seq     uint64 `json:"seq"`
}

func (id ChangeID) String() string { return fmt.Sprintf("%s/%d", id.Replica, id.Seq) }

// Change 是一条不可变的变更记录。
type Change struct {
	ID    ChangeID       `json:"id"`
	Type  ChangeType     `json:"type"`
	Key   string         `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
	// HLC 是产生该变更时的混合逻辑时钟值，用于确定性冲突排序。
	HLC HLC `json:"hlc"`

	// UnknownFields 保留解码时遇到的未知字段，编码时原样带回，
	// 使旧版本副本可以无损中转新版本产生的变更。
	UnknownFields map[string]json.RawMessage `json:"-"`
}

// MarshalJSON 编码 Change，并附带保留的未知字段。
func (c Change) MarshalJSON() ([]byte, error) {
	type alias Change
	base, err := json.Marshal(alias(c))
	if err != nil {
		return nil, err
	}
	if len(c.UnknownFields) == 0 {
		return base, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(base, &m); err != nil {
		return nil, err
	}
	for k, v := range c.UnknownFields {
		if !knownFields[k] {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

// UnmarshalJSON 解码 Change，并捕获未知字段以便后续原样转发。
func (c *Change) UnmarshalJSON(data []byte) error {
	type alias Change
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*c = Change(a)
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for k, v := range m {
		if !knownFields[k] {
			if c.UnknownFields == nil {
				c.UnknownFields = map[string]json.RawMessage{}
			}
			c.UnknownFields[k] = v
		}
	}
	return nil
}

// VersionVector 记录本副本已从每个副本观察到的最大序号，用于去重与增量同步。
type VersionVector map[string]uint64

// Clone 返回深拷贝。
func (vv VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(vv))
	for k, v := range vv {
		out[k] = v
	}
	return out
}

// Has 报告该向量是否已覆盖给定变更（用于幂等去重）。
func (vv VersionVector) Has(id ChangeID) bool {
	return vv[id.Replica] >= id.Seq
}

// Merge 取两个向量的逐分量最大值。
func (vv VersionVector) Merge(other VersionVector) {
	for k, v := range other {
		if vv[k] < v {
			vv[k] = v
		}
	}
}

// String 以稳定顺序输出，便于测试与日志。
func (vv VersionVector) String() string {
	keys := make([]string, 0, len(vv))
	for k := range vv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := "{"
	for i, k := range keys {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s:%d", k, vv[k])
	}
	return out + "}"
}
