package sync

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// 记录格式：[4B 长度][4B CRC32(载荷)][载荷(JSON)]。
// 打开时顺序扫描，遇到长度越界或 CRC 不匹配的记录即视为崩溃造成的
// 撕裂写，截断到上一个有效记录末尾，保证日志始终恢复到一致前缀。
const (
	lenSize  = 4
	crcSize  = 4
	headSize = lenSize + crcSize
)

// Store 是追加式变更日志（WAL）。
type Store struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

// OpenStore 打开（必要时创建）日志文件，并执行崩溃恢复。
func OpenStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	valid, err := scanValid(path)
	if err != nil {
		return nil, err
	}
	if err := os.Truncate(path, valid); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Store{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

// scanValid 返回文件中有效记录前缀的字节长度。
func scanValid(path string) (int64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var off int64
	head := make([]byte, headSize)
	for {
		if _, err := io.ReadFull(f, head); err != nil {
			return off, nil // 头部不完整：截断
		}
		n := binary.BigEndian.Uint32(head[:lenSize])
		if n == 0 || n > 64<<20 {
			return off, nil // 长度非法：截断
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			return off, nil // 载荷不完整：截断
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[lenSize:]) {
			return off, nil // CRC 不匹配：截断
		}
		off += int64(headSize) + int64(n)
	}
}

// Append 追加一条变更并 fsync 落盘，返回前保证崩溃后该记录仍可见。
func (s *Store) Append(c Change) error {
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	var head [headSize]byte
	binary.BigEndian.PutUint32(head[:lenSize], uint32(len(payload)))
	binary.BigEndian.PutUint32(head[lenSize:], crc32.ChecksumIEEE(payload))
	if _, err := s.w.Write(head[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// Replay 按顺序回放全部有效记录。
func (s *Store) Replay(fn func(Change) error) error {
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	head := make([]byte, headSize)
	for {
		if _, err := io.ReadFull(f, head); err != nil {
			return nil
		}
		n := binary.BigEndian.Uint32(head[:lenSize])
		if n == 0 || n > 64<<20 {
			return nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[lenSize:]) {
			return nil
		}
		var c Change
		if err := json.Unmarshal(payload, &c); err != nil {
			return fmt.Errorf("解码变更失败: %w", err)
		}
		if err := fn(c); err != nil {
			return err
		}
	}
}

// Replace 以给定变更序列原子地重建日志（用于压缩）：
// 先写临时文件并 fsync，再 rename 覆盖，崩溃时旧文件仍然完整。
func (s *Store) Replace(changes []Change) error {
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, c := range changes {
		payload, err := json.Marshal(c)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		var head [headSize]byte
		binary.BigEndian.PutUint32(head[:lenSize], uint32(len(payload)))
		binary.BigEndian.PutUint32(head[lenSize:], crc32.ChecksumIEEE(payload))
		if _, err := w.Write(head[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := w.Write(payload); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	// 关闭旧句柄后原子替换，再以追加模式重新打开。
	s.f.Close()
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	nf, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.f = nf
	s.w = bufio.NewWriter(nf)
	return nil
}

// Close 关闭日志文件。
func (s *Store) Close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}
