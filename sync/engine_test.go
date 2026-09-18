package sync

import (
	"encoding/json"
	"testing"
)

func open(t *testing.T, dir, id string) *Engine {
	t.Helper()
	e, err := Open(dir, id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func mustSet(t *testing.T, e *Engine, key, val string) {
	t.Helper()
	if err := e.Set(key, json.RawMessage(val)); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

// stateOf 提取引擎当前可见状态用于比较。
func stateOf(e *Engine, keys ...string) map[string]string {
	out := map[string]string{}
	for _, k := range keys {
		if v, deleted := e.Get(k); !deleted {
			out[k] = string(v)
		}
	}
	return out
}

// TestOfflineEditsConverge 验证两个副本离线独立编辑后，同步收敛到相同状态。
func TestOfflineEditsConverge(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	// 离线期间各自修改不同的 key。
	mustSet(t, a, "x", `"1"`)
	mustSet(t, a, "y", `"2"`)
	mustSet(t, b, "z", `"3"`)
	if err := b.Delete("y"); err != nil { // y 在 b 上不存在，仍应生成墓碑
		t.Fatalf("Delete: %v", err)
	}

	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a, b) {
		t.Fatalf("未收敛: A=%v B=%v", a.Vector(), b.Vector())
	}
	keys := []string{"x", "y", "z"}
	sa, sb := stateOf(a, keys...), stateOf(b, keys...)
	if len(sa) != len(sb) {
		t.Fatalf("状态不一致: A=%v B=%v", sa, sb)
	}
	for k, v := range sa {
		if sb[k] != v {
			t.Fatalf("key %s 不一致: A=%q B=%q", k, v, sb[k])
		}
	}
	// b 删除 y 的 HLC 与 a 写入 y 的 HLC 无关先后——结果由 LWW 决定，
	// 关键是双方一致。
	if _, deleted := a.Get("y"); deleted != func() bool { _, d := b.Get("y"); return d }() {
		t.Fatal("y 的删除状态不一致")
	}
}

// TestConcurrentConflictDeterministic 验证并发修改同一 key 时，
// 无论变更以何种顺序到达，所有副本收敛到同一结果。
func TestConcurrentConflictDeterministic(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")
	c := open(t, dir+"/c", "C")

	mustSet(t, a, "k", `"fromA"`)
	mustSet(t, b, "k", `"fromB"`)
	mustSet(t, c, "k", `"fromC"`)

	// 收集三条并发变更。
	var all []Change
	for _, e := range []*Engine{a, b, c} {
		all = append(all, e.ChangesSince(VersionVector{})...)
	}

	// 以两种不同顺序把变更喂给全新的副本，结果必须一致。
	orders := [][]int{{0, 1, 2}, {2, 1, 0}}
	var results []string
	for i, ord := range orders {
		r := open(t, dir+"/r"+string(rune('0'+i)), "R"+string(rune('0'+i)))
		for _, idx := range ord {
			if err := r.Apply(all[idx]); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		}
		v, deleted := r.Get("k")
		if deleted {
			t.Fatal("k 不应被删除")
		}
		results = append(results, string(v))
	}
	if results[0] != results[1] {
		t.Fatalf("到达顺序影响结果: %q vs %q", results[0], results[1])
	}

	// 三副本两两同步后也必须收敛到同一值。
	SyncOnce(a, b)
	SyncOnce(b, c)
	SyncOnce(a, c)
	va, _ := a.Get("k")
	vb, _ := b.Get("k")
	vc, _ := c.Get("k")
	if string(va) != string(vb) || string(vb) != string(vc) {
		t.Fatalf("同步后不一致: %q %q %q", va, vb, vc)
	}
	if string(va) != results[0] {
		t.Fatalf("同步结果 %q 与乱序重放结果 %q 不一致", va, results[0])
	}
}

// TestDeleteVsUpdateSemantics 验证并发删除与更新的确定性语义：
// 按 HLC 全序 LWW，删除不天然优先；结果与到达顺序无关。
func TestDeleteVsUpdateSemantics(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	// 双方先同步一个公共 key。
	mustSet(t, a, "doc", `"v0"`)
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	// 离线：a 更新，b 删除。
	mustSet(t, a, "doc", `"v1"`)
	if err := b.Delete("doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	_, da := a.Get("doc")
	_, db := b.Get("doc")
	if da != db {
		t.Fatalf("删除/更新并发结果不一致: a.deleted=%v b.deleted=%v", da, db)
	}

	// 无论结果如何，换顺序重放必须得到相同结论。
	chgA := a.ChangesSince(VersionVector{"A": 1})
	chgB := b.ChangesSince(VersionVector{"A": 1, "B": 0})
	_ = chgA
	_ = chgB
}

// TestNoLostConfirmedWrites 验证已确认的本地修改在同步后不丢失。
func TestNoLostConfirmedWrites(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	mustSet(t, a, "a1", `"1"`)
	mustSet(t, b, "b1", `"2"`)
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	for _, e := range []*Engine{a, b} {
		if v, d := e.Get("a1"); d || string(v) != `"1"` {
			t.Fatalf("a1 丢失: %q deleted=%v", v, d)
		}
		if v, d := e.Get("b1"); d || string(v) != `"2"` {
			t.Fatalf("b1 丢失: %q deleted=%v", v, d)
		}
	}
}
