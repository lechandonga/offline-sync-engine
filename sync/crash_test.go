package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestWALTornWriteRecovery 验证日志尾部出现撕裂写（崩溃残留）时，
// 重启能截断到一致前缀，不损坏已落盘的变更，且之后可继续写入。
func TestWALTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")

	mustSet(t, a, "k1", `"1"`)
	mustSet(t, a, "k2", `"2"`)
	wantVec := a.Vector()
	a.Close()

	// 模拟崩溃造成的撕裂写：在日志尾部追加半个记录。
	logPath := filepath.Join(dir, "a", "changes.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开日志: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 100, 1, 2, 3}); err != nil { // 声明长度 100 但只有 3 字节
		t.Fatalf("写入垃圾: %v", err)
	}
	f.Close()

	// 重启：应恢复到撕裂前的一致前缀。
	a2 := open(t, dir+"/a", "A")
	if got := a2.Vector(); got.String() != wantVec.String() {
		t.Fatalf("恢复后版本向量错误: 期望 %v, 得到 %v", wantVec, got)
	}
	if v, d := a2.Get("k2"); d || string(v) != `"2"` {
		t.Fatalf("k2 恢复错误: %q deleted=%v", v, d)
	}

	// 恢复后日志可继续追加且可被其他副本同步。
	mustSet(t, a2, "k3", `"3"`)
	b := open(t, dir+"/b", "B")
	if err := SyncOnce(a2, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a2, b) {
		t.Fatal("撕裂写恢复后无法收敛")
	}
}

// TestCrashBetweenSnapshotAndLogReplace 验证压缩在"快照已落盘、
// 日志尚未截断"之间崩溃时，重启后快照与完整日志叠加恢复，
// 已被快照覆盖的变更按版本向量去重，不会重复应用。
func TestCrashBetweenSnapshotAndLogReplace(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")

	mustSet(t, a, "x", `"1"`)
	mustSet(t, a, "y", `"2"`)

	// 手动执行 Compact 的前半段：只写快照，不替换日志（模拟此刻崩溃）。
	a.mu.RLock()
	snap := Snapshot{Vector: a.vv.Clone(), Entries: map[string]SnapshotEntry{}}
	for k, en := range a.state {
		snap.Entries[k] = SnapshotEntry{Value: en.value, Deleted: en.deleted, HLC: en.hlc}
	}
	a.mu.RUnlock()
	if err := saveSnapshot(a.snapshotPath(), snap); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}
	wantVec := a.Vector()
	a.Close()

	// 重启：快照 + 完整日志回放，重复部分必须幂等。
	a2 := open(t, dir+"/a", "A")
	if got := a2.Vector(); got.String() != wantVec.String() {
		t.Fatalf("版本向量错误: 期望 %v, 得到 %v", wantVec, got)
	}
	assertNoDuplicateIDs(t, a2)
	if v, d := a2.Get("x"); d || string(v) != `"1"` {
		t.Fatalf("x 恢复错误: %q deleted=%v", v, d)
	}
}

// TestCrashLeavesSnapshotTempFile 验证快照写入中途崩溃（残留 .tmp 文件）
// 不影响重启：临时文件被忽略，状态来自旧快照与日志。
func TestCrashLeavesSnapshotTempFile(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	mustSet(t, a, "k", `"v"`)
	wantVec := a.Vector()
	a.Close()

	// 模拟快照写一半崩溃：留下损坏的临时文件。
	tmp := filepath.Join(dir, "a", "snapshot.json.tmp")
	if err := os.WriteFile(tmp, []byte(`{"vector": corrupt`), 0o644); err != nil {
		t.Fatalf("写临时文件: %v", err)
	}

	a2 := open(t, dir+"/a", "A")
	if got := a2.Vector(); got.String() != wantVec.String() {
		t.Fatalf("版本向量错误: 期望 %v, 得到 %v", wantVec, got)
	}
	if v, d := a2.Get("k"); d || string(v) != `"v"` {
		t.Fatalf("k 恢复错误: %q deleted=%v", v, d)
	}
	// 残留临时文件不妨碍后续压缩。
	if err := a2.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
}

// TestCrashAfterLogAppendBeforeApply 验证"先写日志后改内存"的顺序：
// 变更已落盘但进程在应用前退出时，重启回放能补齐该变更，
// 不会出现日志有记录而状态丢失的不一致。
func TestCrashAfterLogAppendBeforeApply(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "a", "changes.log")

	// 只写日志，不经引擎应用（模拟落盘后、应用前崩溃）。
	store, err := OpenStore(logPath)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	c := Change{
		ID:    ChangeID{Replica: "A", Seq: 1},
		Type:  ChangeSet,
		Key:   "durable",
		Value: json.RawMessage(`"yes"`),
		HLC:   HLC{Millis: 1, Replica: "A"},
	}
	if err := store.Append(c); err != nil {
		t.Fatalf("Append: %v", err)
	}
	store.Close()

	// 重启引擎：日志回放必须恢复该变更。
	a := open(t, dir+"/a", "A")
	if v, d := a.Get("durable"); d || string(v) != `"yes"` {
		t.Fatalf("落盘变更丢失: %q deleted=%v", v, d)
	}
	if !a.Vector().Has(c.ID) {
		t.Fatalf("版本向量未覆盖已落盘变更: %v", a.Vector())
	}
}

// TestCrashDuringInstallSnapshot 验证安装远端快照中途崩溃
// （状态已合并但快照文件未持久化）时，重启后从日志与旧快照恢复，
// 重新同步即可收敛，不会损坏同步进度。
func TestCrashDuringInstallSnapshot(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	const n = 6
	for i := 0; i < n; i++ {
		mustSet(t, a, "k"+string(rune('a'+i)), `"v"`)
	}
	// b 只同步到一半并落盘，随后"崩溃"（直接关闭重开）。
	changes := a.ChangesSince(VersionVector{})
	for _, c := range changes[:n/2] {
		if err := b.Apply(c); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	b.Close()

	// b 崩溃期间 a 执行压缩（增量日志被截断，只剩基线快照）。
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 重启 b：已应用的变更来自日志，落后的部分通过基线快照 + 增量补齐。
	b2 := open(t, dir+"/b", "B")
	if err := SyncOnce(a, b2); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a, b2) {
		t.Fatalf("崩溃恢复后未收敛: A=%v B=%v", a.Vector(), b2.Vector())
	}
	assertNoDuplicateIDs(t, b2)
	for _, c := range changes {
		if _, d := b2.Get(c.Key); d {
			t.Fatalf("key %s 丢失", c.Key)
		}
	}
}
