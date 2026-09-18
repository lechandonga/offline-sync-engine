package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// 写入过程中崩溃（日志尾部出现半条记录）：重启后应截断损坏尾部，
// 已确认的完整记录不丢失，状态一致。
func TestCrashRecoveryTruncatesTornTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "R")
	e, err := Open(dir, "R")
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "a", "1")
	mustSet(t, e, "b", "2")
	e.Close()

	// 模拟崩溃：在日志尾部追加半条记录
	logPath := filepath.Join(dir, "changes.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 50, 1, 2, 3}); err != nil { // 声称 50 字节，实际只有 3
		t.Fatal(err)
	}
	f.Close()

	e2, err := Open(dir, "R")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if v, ok := e2.Get("a"); !ok || string(v) != `"1"` {
		t.Fatalf("lost confirmed write a: %s ok=%v", v, ok)
	}
	if v, ok := e2.Get("b"); !ok || string(v) != `"2"` {
		t.Fatalf("lost confirmed write b: %s ok=%v", v, ok)
	}
	// 恢复后应能继续写入并再次重开
	mustSet(t, e2, "c", "3")
	e2.Close()
	e3, err := Open(dir, "R")
	if err != nil {
		t.Fatal(err)
	}
	defer e3.Close()
	if _, ok := e3.Get("c"); !ok {
		t.Fatal("write after recovery lost")
	}
}

// 压缩（Rewrite 原子 rename）过程中崩溃：旧日志仍完整可用。
func TestCrashDuringCompactionKeepsOldLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "R")
	e, err := Open(dir, "R")
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "x", "1")
	mustSet(t, e, "x", "2")
	e.Close()

	// 模拟压缩崩溃：留下 .tmp 文件，主日志未动
	logPath := filepath.Join(dir, "changes.log")
	if err := os.WriteFile(logPath+".tmp", []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(dir, "R")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if v, ok := e2.Get("x"); !ok || string(v) != `"2"` {
		t.Fatalf("state lost after compaction crash: %s ok=%v", v, ok)
	}
}

// 快照后重启：状态与压缩前一致，且新变更继续正常工作。
func TestSnapshotRestartConsistency(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "R")
	e, err := Open(dir, "R")
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "k1", "old")
	mustSet(t, e, "k1", "new")
	mustSet(t, e, "k2", "keep")
	if _, err := e.Delete("k2"); err != nil {
		t.Fatal(err)
	}
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "k3", "post-snap")
	e.Close()

	e2, err := Open(dir, "R")
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if v, ok := e2.Get("k1"); !ok || string(v) != `"new"` {
		t.Fatalf("k1: %s ok=%v", v, ok)
	}
	if _, ok := e2.Get("k2"); ok {
		t.Fatal("k2 should stay deleted")
	}
	if _, ok := e2.Get("k3"); !ok {
		t.Fatal("post-snapshot change lost")
	}
	// 压缩后日志应只含每 key 的获胜者
	if n := len(e2.ChangesSince(nil)); n != 2 { // k1 胜者 + k2 墓碑 + k3 = 3? 见下断言
		t.Logf("compacted log size = %d", n)
	}
	if got := len(e2.ChangesSince(nil)); got != 3 {
		t.Fatalf("expected 3 changes after compaction (k1 winner, k2 tombstone, k3), got %d", got)
	}
	_ = json.RawMessage(nil)
}
