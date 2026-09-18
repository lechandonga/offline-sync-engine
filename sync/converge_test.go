package sync

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T, replica string) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), replica), replica)
	if err != nil {
		t.Fatalf("open %s: %v", replica, err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func mustSet(t *testing.T, e *Engine, key, val string) {
	t.Helper()
	if _, err := e.Set(key, json.RawMessage(fmt.Sprintf("%q", val))); err != nil {
		t.Fatalf("set: %v", err)
	}
}

func stateOf(t *testing.T, e *Engine) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, k := range e.Keys() {
		v, _ := e.Get(k)
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		out[k] = s
	}
	return out
}

// 三个副本离线各自编辑，两两同步后必须收敛到相同状态。
func TestOfflineEditsConverge(t *testing.T) {
	a := openTemp(t, "A")
	b := openTemp(t, "B")
	c := openTemp(t, "C")

	mustSet(t, a, "x", "from-a")
	mustSet(t, b, "y", "from-b")
	mustSet(t, c, "z", "from-c")
	mustSet(t, a, "shared", "a1")
	mustSet(t, b, "shared", "b1")
	if _, err := c.Delete("shared"); err != nil {
		t.Fatal(err)
	}

	// 网状两两同步（顺序不应影响最终结果）
	for _, pair := range [][2]*Engine{{a, b}, {b, c}, {a, c}, {a, b}, {b, c}} {
		if err := SyncPair(pair[0], pair[1]); err != nil {
			t.Fatalf("sync: %v", err)
		}
	}

	sa, sb, sc := stateOf(t, a), stateOf(t, b), stateOf(t, c)
	if fmt.Sprint(sa) != fmt.Sprint(sb) || fmt.Sprint(sb) != fmt.Sprint(sc) {
		t.Fatalf("diverged: A=%v B=%v C=%v", sa, sb, sc)
	}
	if sa["x"] != "from-a" || sa["y"] != "from-b" || sa["z"] != "from-c" {
		t.Fatalf("lost confirmed writes: %v", sa)
	}
}
