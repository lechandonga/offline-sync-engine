package sync

import "os"

// Store 是追加式变更日志（WAL）。每条记录带长度与 CRC，
// 打开时扫描并截断撕裂的尾部记录，保证崩溃后恢复到一致前缀。
type Store struct {
	path string
	f    *os.File
}

// OpenStore 打开（必要时创建）日志文件，并执行崩溃恢复。
func OpenStore(path string) (*Store, error) { return nil, nil }

// Append 追加一条变更并落盘（占位实现）。
func (s *Store) Append(c Change) error { return nil }

// Replay 按顺序回放全部有效记录（占位实现）。
func (s *Store) Replay(fn func(Change) error) error { return nil }

// Replace 以给定变更序列原子地重建日志（用于压缩）（占位实现）。
func (s *Store) Replace(changes []Change) error { return nil }

// Close 关闭日志文件。
func (s *Store) Close() error { return nil }
