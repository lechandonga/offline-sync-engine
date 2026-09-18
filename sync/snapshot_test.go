package sync

import (
	"encoding/json"
	"testing"
)

// TestCompactDuringSync 验证在同步进行到一半时执行压缩，
// 不丢失也不重复应用变更，最终仍收敛。
func TestCompactDuringSync(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	const n = 10
	for i := 0; i < n; i++ {
		mustSet(t, a, "k"+string(rune('a'+i)), `"v"`)
	}
	changes := a.ChangesSince(VersionVector{})

	// 同步一半后在发送方压缩。
	for _, c := range changes[:n/2] {
		if err := b.Apply(c); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 继续同步直至收敛。
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a, b) {
		t.Fatalf("压缩后未收敛: A=%v B=%v", a.Vector(), b.Vector())
	}
	assertNoDuplicateIDs(t, b)
	for _, c := range changes {
		if _, d := b.Get(c.Key); d {
			t.Fatalf("key %s 在压缩后丢失", c.Key)
		}
	}
}

// TestCompactWhileWriting 验证压缩与本地新写入交错时不丢不重。
func TestCompactWhileWriting(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	mustSet(t, a, "before", `"1"`)
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// 压缩后继续写入。
	mustSet(t, a, "after", `"2"`)

	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	for _, k := range []string{"before", "after"} {
		if _, d := b.Get(k); d {
			t.Fatalf("key %s 丢失", k)
		}
	}
	assertNoDuplicateIDs(t, b)
}

// TestSnapshotRecovery 验证快照 + 日志尾部的重启恢复：
// 快照前的变更来自快照，快照后的变更来自日志，合并后状态一致。
func TestSnapshotRecovery(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")

	mustSet(t, a, "old", `"1"`)
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	mustSet(t, a, "new", `"2"`)
	if err := a.Delete("old"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	wantVec := a.Vector()
	a.Close()

	// 重新打开：应从快照 + 日志恢复出一致状态。
	a2 := open(t, dir+"/a", "A")
	if got := a2.Vector(); got.String() != wantVec.String() {
		t.Fatalf("版本向量恢复错误: 期望 %v, 得到 %v", wantVec, got)
	}
	if _, d := a2.Get("old"); !d {
		t.Fatal("old 应处于已删除状态")
	}
	if v, d := a2.Get("new"); d || string(v) != `"2"` {
		t.Fatalf("new 恢复错误: %q deleted=%v", v, d)
	}

	// 恢复后还能与其他副本正常增量同步。
	b := open(t, dir+"/b", "B")
	if err := SyncOnce(a2, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a2, b) {
		t.Fatal("恢复后无法收敛")
	}
}

// TestCompactPreservesUnknownChanges 验证压缩保留未知类型变更，
// 使未来版本升级后仍能合并这些数据。
func TestCompactPreservesUnknownChanges(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")

	mustSet(t, a, "k", `"v"`)
	// 注入一条未知类型变更（模拟新版本副本产生的数据）。
	unknown := Change{
		ID:   ChangeID{Replica: "FUTURE", Seq: 1},
		Type: ChangeType("merge-op"),
		Key:  "k",
		HLC:  HLC{Millis: 1, Replica: "FUTURE"},
		Value: json.RawMessage(`{"op":"merge"}`),
	}
	if err := a.Apply(unknown); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 未知变更在压缩后仍可用于同步。
	found := false
	for _, c := range a.ChangesSince(VersionVector{}) {
		if c.ID.Replica == "FUTURE" {
			found = true
		}
	}
	if !found {
		t.Fatal("压缩丢失了未知类型变更")
	}
}

// assertNoDuplicateIDs 断言副本变更日志中没有重复应用的变更。
func assertNoDuplicateIDs(t *testing.T, e *Engine) {
	t.Helper()
	seen := map[ChangeID]bool{}
	for _, c := range e.ChangesSince(VersionVector{}) {
		if seen[c.ID] {
			t.Fatalf("变更 %s 被重复应用", c.ID)
		}
		seen[c.ID] = true
	}
}
