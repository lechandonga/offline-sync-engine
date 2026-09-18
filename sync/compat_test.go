package sync

import (
	"encoding/json"
	"fmt"
	"testing"
)

// 未识别的变更类型：不破坏本地状态，且会被保留并转发给其他副本。
func TestUnknownOpTypePreservedAndForwarded(t *testing.T) {
	old1 := openTemp(t, "old1")
	old2 := openTemp(t, "old2")

	// 模拟新版本产生的变更（旧版本不识别 "merge" 类型）
	future := Change{
		ID: "new-1", Replica: "new", Seq: 1, Key: "doc",
		Type:  OpType("merge"),
		Value: json.RawMessage(`{"ops":[1,2]}`),
	}
	if err := old1.Receive(future); err != nil {
		t.Fatal(err)
	}
	// 本地状态不受影响（未知类型不改变物化状态）
	if _, ok := old1.Get("doc"); ok {
		t.Fatal("unknown op should not mutate state")
	}
	// 但变更被保留，可转发给另一个旧副本
	if err := SyncPair(old1, old2); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range old2.ChangesSince(nil) {
		if c.ID == "new-1" && c.Type == OpType("merge") {
			found = true
		}
	}
	if !found {
		t.Fatal("unknown change not forwarded")
	}
	// 压缩也不得丢弃未知类型
	if err := old2.Snapshot(); err != nil {
		t.Fatal(err)
	}
	found = false
	for _, c := range old2.ChangesSince(nil) {
		if c.ID == "new-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("unknown change lost during compaction")
	}
}

// 未识别的数据字段：反序列化后原样保留，重新序列化不丢失。
func TestUnknownFieldsRoundTrip(t *testing.T) {
	raw := []byte(`{"id":"n-1","replica":"n","seq":1,"key":"k","type":"set","value":"1","future_field":{"a":1},"another":"x"}`)
	var c Change
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Extra) != 2 {
		t.Fatalf("expected 2 extra fields, got %v", c.Extra)
	}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if string(m["future_field"]) != `{"a":1}` || string(m["another"]) != `"x"` {
		t.Fatalf("extra fields lost: %s", out)
	}
	// 经过日志持久化 + 重启后仍保留
	e := openTemp(t, "R")
	if err := e.Receive(c); err != nil {
		t.Fatal(err)
	}
	e.Close()
	dir := e.dir
	e2, err := Open(dir, "R")
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	for _, got := range e2.ChangesSince(nil) {
		if got.ID == "n-1" {
			if len(got.Extra) != 2 {
				t.Fatalf("extra fields lost across restart: %v", got.Extra)
			}
			return
		}
	}
	t.Fatal("change missing after restart")
}

// 混合版本集群：新副本的未知变更经旧副本中转后，
// 另一个升级后的副本应能正确合并此前保留的数据。
func TestMixedVersionClusterMergesAfterUpgrade(t *testing.T) {
	oldA := openTemp(t, "A")
	oldB := openTemp(t, "B")

	// "新版本" 变更经 A 中转到达 B
	future := Change{
		ID: "N-1", Replica: "N", Seq: 1, Key: "k",
		Type: OpType("set"), Value: json.RawMessage(`"v"`),
		Extra: map[string]json.RawMessage{"crdt_meta": json.RawMessage(`{"node":7}`)},
	}
	if err := oldA.Receive(future); err != nil {
		t.Fatal(err)
	}
	if err := SyncPair(oldA, oldB); err != nil {
		t.Fatal(err)
	}
	// B（升级后）重新读取：字段与值完整可合并
	for _, c := range oldB.ChangesSince(nil) {
		if c.ID == "N-1" {
			if string(c.Extra["crdt_meta"]) != `{"node":7}` {
				t.Fatalf("metadata lost: %v", c.Extra)
			}
			return
		}
	}
	t.Fatal(fmt.Sprintf("change not preserved through old replica"))
}
