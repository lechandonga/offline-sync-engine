package sync

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
)

// Engine 是单个副本的同步引擎，组合日志、物化状态与快照。
// 所有公开方法均可并发调用。
type Engine struct {
	mu      sync.Mutex
	replica string
	seq     uint64
	log     *Log
	store   *Store
	changes []Change // 全部已持久化变更（含压缩后保留集）
	dir     string
}

// Open 在 dir 下打开（或创建）副本引擎，并执行崩溃恢复：
// 重放日志重建物化状态，损坏的尾部记录已被截断。
func Open(dir, replica string) (*Engine, error) {
	l, err := OpenLog(filepath.Join(dir, "changes.log"))
	if err != nil {
		return nil, err
	}
	e := &Engine{replica: replica, log: l, store: NewStore(), dir: dir}
	err = l.Replay(func(c Change) error {
		e.store.Apply(c)
		e.changes = append(e.changes, c)
		if c.Replica == replica && c.Seq > e.seq {
			e.seq = c.Seq
		}
		return nil
	})
	if err != nil {
		l.Close()
		return nil, err
	}
	return e, nil
}

// Set 本地写入一个键值，产生并持久化一条变更。
func (e *Engine) Set(key string, value json.RawMessage) (Change, error) {
	return e.mutate(key, OpSet, value)
}

// Delete 本地删除一个键，产生并持久化一条墓碑变更。
func (e *Engine) Delete(key string) (Change, error) {
	return e.mutate(key, OpDelete, nil)
}

func (e *Engine) mutate(key string, t OpType, value json.RawMessage) (Change, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq++
	c := Change{
		ID:      fmt.Sprintf("%s-%d", e.replica, e.seq),
		Replica: e.replica,
		Seq:     e.seq,
		Key:     key,
		Type:    t,
		Value:   value,
	}
	// 先落盘再改内存：崩溃时重启从日志重建，不会出现半应用状态。
	if err := e.log.Append(c); err != nil {
		return Change{}, err
	}
	e.store.Apply(c)
	e.changes = append(e.changes, c)
	return c, nil
}

// Get 读取当前物化状态。
func (e *Engine) Get(key string) (json.RawMessage, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.Get(key)
}

// Receive 应用一条来自远端的变更（幂等、可乱序、可重复投递）。
func (e *Engine) Receive(c Change) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.store.Seen(c.ID) {
		return nil // 重复投递：幂等忽略
	}
	if err := e.log.Append(c); err != nil {
		return err
	}
	e.store.Apply(c)
	e.changes = append(e.changes, c)
	return nil
}

// ChangesSince 返回本副本已知、而给定版本向量缺失的变更（增量同步）。
func (e *Engine) ChangesSince(known map[string]uint64) []Change {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Change
	for _, c := range e.changes {
		if c.Seq > known[c.Replica] {
			out = append(out, c)
		}
	}
	return out
}

// VersionVector 返回本副本已见的版本向量（replica -> 最大连续 seq）。
func (e *Engine) VersionVector() map[string]uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := map[string]map[uint64]bool{}
	for _, c := range e.changes {
		m, ok := seen[c.Replica]
		if !ok {
			m = map[uint64]bool{}
			seen[c.Replica] = m
		}
		m[c.Seq] = true
	}
	vv := map[string]uint64{}
	for r, m := range seen {
		var n uint64
		for m[n+1] {
			n++
		}
		vv[r] = n
	}
	return vv
}

// Snapshot 生成快照并压缩日志。压缩期间被阻塞的新变更在压缩后
// 正常追加，不会丢失或重复；重写是原子 rename，崩溃安全。
func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	kept := Compact(e.changes)
	if err := e.log.Rewrite(kept); err != nil {
		return err
	}
	e.changes = append([]Change(nil), kept...)
	return nil
}

// Close 关闭引擎。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.log.Close()
}

// Keys 返回当前可见的键集合（已排序）。
func (e *Engine) Keys() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.Keys()
}
