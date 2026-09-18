package sync

import "encoding/json"

// Engine 是单个副本的同步引擎：管理本地状态、变更日志与版本向量。
type Engine struct {
	replica string
	clock   *Clock
	store   *Store
}

// Open 在指定目录打开（或创建）一个副本引擎，并执行崩溃恢复（占位实现）。
func Open(dir, replicaID string) (*Engine, error) { return nil, nil }

// Set 本地写入 key（占位实现）。
func (e *Engine) Set(key string, value json.RawMessage) error { return nil }

// Delete 本地删除 key（写入墓碑）（占位实现）。
func (e *Engine) Delete(key string) error { return nil }

// Get 读取当前 key 的值；deleted 为 true 表示该 key 已被删除或不存在。
func (e *Engine) Get(key string) (value json.RawMessage, deleted bool) { return nil, true }

// Apply 幂等地应用一条远端变更（占位实现）。
func (e *Engine) Apply(c Change) error { return nil }

// Vector 返回当前版本向量的拷贝。
func (e *Engine) Vector() VersionVector { return nil }

// ChangesSince 返回 since 之后本副本已知的全部变更（增量同步用）（占位实现）。
func (e *Engine) ChangesSince(since VersionVector) []Change { return nil }

// Close 关闭引擎。
func (e *Engine) Close() error { return nil }
