package sync

import "sort"

// Snapshot 是对某一时刻物化状态 + 已见变更集合的持久化快照，
// 用于控制日志体积与加载成本。
type Snapshot struct {
	Changes []Change `json:"changes"` // 压缩后仍需保留的变更（每 key 的获胜者 + 未知类型）
}

// Compact 计算压缩结果：对每个 key 仅保留裁决获胜的变更，
// 未识别的变更类型一律保留（保证版本互通不丢数据）。
// 输入可包含压缩期间新产生的变更：它们要么成为获胜者、
// 要么（若是更新更旧的 key）按同一全序被正确淘汰，不会丢失或重复。
// 输出按 (Key, Seq, Replica, ID) 排序，保证结果确定。
func Compact(changes []Change) []Change {
	winner := map[string]Change{}
	var kept []Change
	for _, c := range changes {
		switch c.Type {
		case OpSet, OpDelete:
			cur, ok := winner[c.Key]
			if !ok || Compare(cur, c) < 0 {
				winner[c.Key] = c
			}
		default:
			kept = append(kept, c) // 未知类型原样保留
		}
	}
	for _, w := range winner {
		kept = append(kept, w)
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].Key != kept[j].Key {
			return kept[i].Key < kept[j].Key
		}
		return Compare(kept[i], kept[j]) < 0
	})
	return kept
}
