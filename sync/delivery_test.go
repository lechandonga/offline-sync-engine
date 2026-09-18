package sync

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

// 重复投递与乱序到达不得造成重复应用或状态分叉。
func TestDuplicateAndOutOfOrderDelivery(t *testing.T) {
	src := openTemp(t, "src")
	dst := openTemp(t, "dst")

	const n = 100
	for i := 0; i < n; i++ {
		mustSet(t, src, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i))
	}

	changes := src.ChangesSince(nil)
	// 乱序 + 每条重复投递多次
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(changes), func(i, j int) { changes[i], changes[j] = changes[j], changes[i] })
	for round := 0; round < 3; round++ {
		for _, c := range changes {
			if err := dst.Receive(c); err != nil {
				t.Fatalf("receive: %v", err)
			}
		}
	}

	if got := len(dst.ChangesSince(nil)); got != n {
		t.Fatalf("expected %d changes applied once, got %d", n, got)
	}
	sa, sb := stateOf(t, src), stateOf(t, dst)
	if fmt.Sprint(sa) != fmt.Sprint(sb) {
		t.Fatalf("diverged: %v vs %v", sa, sb)
	}
}

// 中途断连（部分投递）后恢复，应能继续完成同步。
func TestResumeAfterDisconnect(t *testing.T) {
	a := openTemp(t, "A")
	b := openTemp(t, "B")
	for i := 0; i < 10; i++ {
		mustSet(t, a, fmt.Sprintf("k%d", i), "v")
	}
	// 模拟断连：只投递前 4 条
	for _, c := range a.ChangesSince(nil)[:4] {
		if err := b.Receive(c); err != nil {
			t.Fatal(err)
		}
	}
	// 恢复后完整同步
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(stateOf(t, a)) != fmt.Sprint(stateOf(t, b)) {
		t.Fatal("diverged after resume")
	}
	if _, ok := b.Get("k9"); !ok {
		t.Fatal("missing k9 after resume")
	}
}

// 同步必须是增量的：第二轮同步不应重复传输已确认的变更。
func TestIncrementalSync(t *testing.T) {
	a := openTemp(t, "A")
	b := openTemp(t, "B")
	mustSet(t, a, "k1", "v1")
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	mustSet(t, a, "k2", "v2")
	delta := a.ChangesSince(b.VersionVector())
	if len(delta) != 1 || delta[0].Key != "k2" {
		t.Fatalf("expected only k2 in delta, got %v", delta)
	}
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	if v, ok := b.Get("k2"); !ok || string(v) != `"v2"` {
		t.Fatalf("k2 not synced: %s ok=%v", v, ok)
	}
	_ = json.RawMessage(nil)
}
