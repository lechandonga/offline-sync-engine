package sync

import (
	"encoding/json"
	"sort"
)

// Store 是基于变更日志的确定性物化状态。
// 冲突裁决规则（LWW，全序由 Compare 定义）：
//   - 同一 key 的并发 set：按 Compare 取较大者，结果与到达顺序无关。
//   - 并发 delete 与 set：delete 作为带排序键的墓碑参与同一全序，
//     即"删除也带时间戳"，与 set 按同一规则裁决，语义稳定；
//     不存在"删除优先"或"更新优先"的方向性偏袒。
//   - 墓碑在压缩时可随快照一并回收。
type Store struct {
	applied map[string]struct{} // 已应用变更 ID，用于幂等去重
	winner  map[string]Change   // 每个 key 当前获胜的变更（含墓碑）
}

// NewStore 创建空 Store。
func NewStore() *Store {
	return &Store{
		applied: map[string]struct{}{},
		winner:  map[string]Change{},
	}
}

// Apply 应用一条变更。重复应用同一变更（按 ID）是幂等无操作。
// 未知操作类型被记录为已见（保证后续可转发）但不改变状态。
// 返回 true 表示该变更是首次见到。
func (s *Store) Apply(c Change) bool {
	if _, ok := s.applied[c.ID]; ok {
		return false
	}
	s.applied[c.ID] = struct{}{}
	switch c.Type {
	case OpSet, OpDelete:
		cur, ok := s.winner[c.Key]
		if !ok || Compare(cur, c) < 0 {
			s.winner[c.Key] = c
		}
	default:
		// 未识别类型：仅登记 ID，不改变物化状态。
	}
	return true
}

// Get 返回 key 当前值，ok=false 表示不存在或已删除。
func (s *Store) Get(key string) (json.RawMessage, bool) {
	w, ok := s.winner[key]
	if !ok || w.Type == OpDelete {
		return nil, false
	}
	return w.Value, true
}

// Keys 返回当前可见的键集合（已排序）。
func (s *Store) Keys() []string {
	var keys []string
	for k, w := range s.winner {
		if w.Type != OpDelete {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Seen 报告变更 ID 是否已应用过。
func (s *Store) Seen(id string) bool {
	_, ok := s.applied[id]
	return ok
}
