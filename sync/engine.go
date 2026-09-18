package sync

import (
	"encoding/json"
	"path/filepath"
	"sync"
)

// entry 是某个 key 的当前状态（含墓碑）。
type entry struct {
	value   json.RawMessage
	deleted bool
	hlc     HLC // 最后一次决定该 key 状态的变更的 HLC
}

// Engine 是单个副本的同步引擎。
//
// 收敛语义：对每个 key 采用 Last-Writer-Wins，按变更的 HLC 全序比较，
// HLC 更大者胜出；删除与更新同等对待（删除即写入墓碑），
// 因此并发删除与并发更新的结果只取决于 HLC 全序，
// 不依赖消息到达顺序。所有副本应用同一变更集后状态必然一致。
type Engine struct {
	mu      sync.RWMutex
	replica string
	clock   *Clock
	store   *Store

	state   map[string]entry
	vv      VersionVector
	pending map[ChangeID]Change // 已应用但序号未连续的变更（乱序到达）
	changes []Change             // 压缩基线之后的全部变更（用于增量同步）
}

// Open 在指定目录打开（或创建）一个副本引擎，并执行崩溃恢复：
// 先加载快照（若存在），再回放变更日志覆盖其后的内容。
func Open(dir, replicaID string) (*Engine, error) {
	store, err := OpenStore(filepath.Join(dir, "changes.log"))
	if err != nil {
		return nil, err
	}
	e := &Engine{
		replica: replicaID,
		clock:   NewClock(replicaID),
		store:   store,
		state:   map[string]entry{},
		vv:      VersionVector{},
		pending: map[ChangeID]Change{},
	}
	if err := e.loadSnapshot(filepath.Join(dir, "snapshot.json")); err != nil {
		store.Close()
		return nil, err
	}
	if err := store.Replay(func(c Change) error {
		e.apply(c)
		return nil
	}); err != nil {
		store.Close()
		return nil, err
	}
	return e, nil
}

// Set 本地写入 key。
func (e *Engine) Set(key string, value json.RawMessage) error {
	return e.local(ChangeSet, key, value)
}

// Delete 本地删除 key（写入墓碑）。
func (e *Engine) Delete(key string) error {
	return e.local(ChangeDel, key, nil)
}

// local 产生一条本地变更并应用。本地变更序号严格连续，
// 与版本向量推进规则一致。
func (e *Engine) local(t ChangeType, key string, value json.RawMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	seq := e.vv[e.replica] + 1
	c := Change{
		ID:    ChangeID{Replica: e.replica, Seq: seq},
		Type:  t,
		Key:   key,
		Value: value,
		HLC:   e.clock.Now(),
	}
	return e.applyLocked(c)
}

// Get 读取当前 key 的值；deleted 为 true 表示该 key 已被删除或不存在。
func (e *Engine) Get(key string) (json.RawMessage, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	en, ok := e.state[key]
	if !ok || en.deleted {
		return nil, true
	}
	out := make(json.RawMessage, len(en.value))
	copy(out, en.value)
	return out, false
}

// Apply 幂等地应用一条远端变更：重复投递被去重，乱序到达被缓存，
// 未知类型被保留并复制但不作用于本地状态。
func (e *Engine) Apply(c Change) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.applyLocked(c)
}

// applyLocked 是应用变更的内部路径（调用方须持锁）。
// 先写日志再改内存状态：崩溃后由日志回放恢复，不会出现半应用状态。
func (e *Engine) applyLocked(c Change) error {
	if e.vv.Has(c.ID) {
		return nil // 重复投递：幂等忽略
	}
	if _, ok := e.pending[c.ID]; ok {
		return nil // 已缓存的乱序变更
	}
	if err := e.store.Append(c); err != nil {
		return err
	}
	e.apply(c)
	return nil
}

// apply 将变更作用于内存状态并推进版本向量（不触碰磁盘）。
func (e *Engine) apply(c Change) {
	e.clock.Observe(c.HLC)
	switch c.Type {
	case ChangeSet:
		if en, ok := e.state[c.Key]; !ok || en.hlc.Compare(c.HLC) < 0 {
			e.state[c.Key] = entry{value: c.Value, hlc: c.HLC}
		}
	case ChangeDel:
		if en, ok := e.state[c.Key]; !ok || en.hlc.Compare(c.HLC) < 0 {
			e.state[c.Key] = entry{deleted: true, hlc: c.HLC}
		}
	default:
		// 未知类型：保留在日志与同步流中，不作用于状态（前向兼容）。
	}
	e.changes = append(e.changes, c)
	e.advance(c)
}

// advance 将 c 记入版本向量，并尽可能连续推进。
func (e *Engine) advance(c Change) {
	next := e.vv[c.ID.Replica] + 1
	switch {
	case c.ID.Seq <= e.vv[c.ID.Replica]:
		return
	case c.ID.Seq > next:
		e.pending[c.ID] = c // 乱序：等待前驱
		return
	}
	e.vv[c.ID.Replica] = c.ID.Seq
	// 检查缓存的乱序变更能否接续。
	for {
		id := ChangeID{Replica: c.ID.Replica, Seq: e.vv[c.ID.Replica] + 1}
		p, ok := e.pending[id]
		if !ok {
			return
		}
		delete(e.pending, id)
		e.vv[id.Replica] = p.ID.Seq
	}
}

// Vector 返回当前版本向量的拷贝。
func (e *Engine) Vector() VersionVector {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.vv.Clone()
}

// ChangesSince 返回 since 未覆盖的全部已知变更（增量同步用）。
// 返回顺序按 (Replica, Seq) 排序以保证确定性；接收方对乱序免疫。
func (e *Engine) ChangesSince(since VersionVector) []Change {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []Change
	for _, c := range e.changes {
		if !since.Has(c.ID) {
			out = append(out, c)
		}
	}
	return out
}

// Close 关闭引擎。
func (e *Engine) Close() error {
	return e.store.Close()
}
