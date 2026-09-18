package sync

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Log 是追加式变更日志，提供持久化与崩溃恢复。
// 记录格式：4 字节长度 + 4 字节 CRC32 + JSON 负载。
// 加载时遇到截断或校验失败的尾部记录即停止并截断，
// 保证崩溃后不会出现"应用了一半"的变更。
type Log struct {
	path string
	f    *os.File
}

// OpenLog 打开（必要时创建）日志文件，并执行崩溃恢复：
// 扫描到第一条损坏/截断记录即截断其后的全部内容。
func OpenLog(path string) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	valid, err := scanValid(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &Log{path: path, f: f}, nil
}

// scanValid 返回文件中连续有效记录的总字节数。
func scanValid(path string) (int64, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var off int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return off, nil // 头部不完整：视为截断尾部
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		crc := binary.BigEndian.Uint32(hdr[4:])
		if n > 64<<20 {
			return off, nil // 长度异常：视为损坏
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return off, nil
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return off, nil
		}
		off += 8 + int64(n)
	}
}

// Append 追加一条变更并落盘（fsync）。
func (l *Log) Append(c Change) error {
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	rec := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(rec[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(payload))
	copy(rec[8:], payload)
	if _, err := l.f.Write(rec); err != nil {
		return err
	}
	return l.f.Sync()
}

// Replay 按顺序回放所有有效记录。
func (l *Log) Replay(fn func(Change) error) error {
	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return nil // EOF 或截断尾部
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		crc := binary.BigEndian.Uint32(hdr[4:])
		if n > 64<<20 {
			return nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return nil
		}
		var c Change
		if err := json.Unmarshal(payload, &c); err != nil {
			return err
		}
		if err := fn(c); err != nil {
			return err
		}
	}
}

// Rewrite 将给定变更集合原子重写为日志全部内容（用于压缩）：
// 先写临时文件并 fsync，再 rename 覆盖，崩溃时旧文件仍完整。
func (l *Log) Rewrite(changes []Change) error {
	tmp := l.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, c := range changes {
		payload, err := json.Marshal(c)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		rec := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(rec[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(payload))
		copy(rec[8:], payload)
		if _, err := f.Write(rec); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
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
	if err := l.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	nf, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f = nf
	return syncDir(filepath.Dir(l.path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close 关闭日志文件。
func (l *Log) Close() error {
	return l.f.Close()
}
