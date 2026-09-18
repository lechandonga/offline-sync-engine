package sync

import (
	"encoding/json"
	"math/rand"
	"testing"
)

// TestDuplicateDelivery 验证变更重复投递不会造成重复应用或状态分叉。
func TestDuplicateDelivery(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	mustSet(t, a, "k", `"v"`)
	changes := a.ChangesSince(VersionVector{})
	if len(changes) != 1 {
		t.Fatalf("期望 1 条变更, 得到 %d", len(changes))
	}
	// 同一条变更投递三次。
	for i := 0; i < 3; i++ {
		if err := b.Apply(changes[0]); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if got := b.ChangesSince(VersionVector{}); len(got) != 1 {
		t.Fatalf("重复投递导致重复应用: %d 条", len(got))
	}
	if v, d := b.Get("k"); d || string(v) != `"v"` {
		t.Fatalf("状态错误: %q deleted=%v", v, d)
	}
	if !Converged(a, b) {
		t.Fatal("重复投递后未收敛")
	}
}

// TestOutOfOrderDelivery 验证乱序到达不会造成状态分叉或进度损坏。
func TestOutOfOrderDelivery(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	const n = 20
	for i := 0; i < n; i++ {
		mustSet(t, a, "k"+string(rune('a'+i)), json.Number(string(rune('0'+i%10))).String())
	}
	changes := a.ChangesSince(VersionVector{})
	if len(changes) != n {
		t.Fatalf("期望 %d 条变更, 得到 %d", n, len(changes))
	}

	// 随机打乱顺序投递。
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(changes), func(i, j int) { changes[i], changes[j] = changes[j], changes[i] })
	for _, c := range changes {
		if err := b.Apply(c); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	if !Converged(a, b) {
		t.Fatalf("乱序投递后版本向量不一致: A=%v B=%v", a.Vector(), b.Vector())
	}
	for _, c := range changes {
		va, _ := a.Get(c.Key)
		vb, _ := b.Get(c.Key)
		if string(va) != string(vb) {
			t.Fatalf("key %s 分叉: %q vs %q", c.Key, va, vb)
		}
	}
}

// TestDisconnectResume 验证同步中途断连后可继续完成，且不丢不重。
func TestDisconnectResume(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	const n = 10
	for i := 0; i < n; i++ {
		mustSet(t, a, "k"+string(rune('a'+i)), `"x"`)
	}
	changes := a.ChangesSince(VersionVector{})

	// 模拟断连：只投递前一半。
	for _, c := range changes[:n/2] {
		if err := b.Apply(c); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if Converged(a, b) {
		t.Fatal("半同步不应收敛")
	}

	// 恢复连接：再次完整同步。
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a, b) {
		t.Fatal("断连恢复后未收敛")
	}
	// 每条变更在 b 上只被应用一次。
	if got := len(b.ChangesSince(VersionVector{})); got != n {
		t.Fatalf("变更被重复应用: 期望 %d, 得到 %d", n, got)
	}
	for _, c := range changes {
		if _, d := b.Get(c.Key); d {
			t.Fatalf("key %s 丢失", c.Key)
		}
	}
}

// TestBidirectionalSync 验证双向同步后双方变更都完整保留。
func TestBidirectionalSync(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	mustSet(t, a, "fromA", `"1"`)
	mustSet(t, b, "fromB", `"2"`)
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	for _, e := range []*Engine{a, b} {
		if _, d := e.Get("fromA"); d {
			t.Fatal("fromA 丢失")
		}
		if _, d := e.Get("fromB"); d {
			t.Fatal("fromB 丢失")
		}
	}
}
