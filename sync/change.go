// Package sync 提供本地优先的多副本离线数据同步能力。
package sync

import (
	"bytes"
	"encoding/json"
	"sort"
)

// OpType 描述变更操作类型。未知类型会被保留并转发，但不应用到本地状态，
// 以支持新旧版本副本互通。
type OpType string

const (
	OpSet    OpType = "set"
	OpDelete OpType = "del"
)

// Change 是一条不可变的变更记录。
type Change struct {
	ID      string                     `json:"id"`      // 全局唯一变更 ID
	Replica string                     `json:"replica"` // 产生该变更的副本 ID
	Seq     uint64                     `json:"seq"`     // 副本内单调递增序号
	Key     string                     `json:"key"`     // 数据键
	Type    OpType                     `json:"type"`    // 操作类型
	Value   json.RawMessage            `json:"value"`   // 值（delete 时为空）
	Extra   map[string]json.RawMessage `json:"-"`       // 未识别字段，原样保留以兼容新版本
}

// knownFields 用于反序列化时分离未知字段。
var knownFields = map[string]bool{
	"id": true, "replica": true, "seq": true, "key": true, "type": true, "value": true,
}

// Compare 提供确定性的全序，用于冲突裁决，与到达顺序、节点身份无关
// （仅以变更自身内容排序）。排序键为 (Seq, Replica, ID)：
// Seq 反映副本内因果先后；并发变更以 Replica、ID 字典序打破平局，
// 所有副本对同一对变更得出相同结论。
func Compare(a, b Change) int {
	if a.Seq != b.Seq {
		if a.Seq < b.Seq {
			return -1
		}
		return 1
	}
	if a.Replica != b.Replica {
		if a.Replica < b.Replica {
			return -1
		}
		return 1
	}
	return bytes.Compare([]byte(a.ID), []byte(b.ID))
}

// changeAlias 避免 MarshalJSON 递归。
type changeAlias Change

// MarshalJSON 序列化变更，保留未知字段。
func (c Change) MarshalJSON() ([]byte, error) {
	base, err := json.Marshal(changeAlias(c))
	if err != nil {
		return nil, err
	}
	if len(c.Extra) == 0 {
		return base, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(base, &m); err != nil {
		return nil, err
	}
	for k, v := range c.Extra {
		if !knownFields[k] {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

// UnmarshalJSON 反序列化变更，捕获未知字段到 Extra。
func (c *Change) UnmarshalJSON(data []byte) error {
	var a changeAlias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*c = Change(a)
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	var keys []string
	for k := range m {
		if !knownFields[k] {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		c.Extra = make(map[string]json.RawMessage, len(keys))
		for _, k := range keys {
			c.Extra[k] = m[k]
		}
	}
	return nil
}
