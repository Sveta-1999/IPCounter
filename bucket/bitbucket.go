package bucket

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"ipcounter/utils" 
)

const (
	numBuckets    = 256            // split by top 8 bits
	suffixBits    = 24             // per-bucket space = 2^24
	wordsPerSet   = (1<<suffixBits + 31) / 32 // 2^24 bits / 32
	writeBufSize  = 256 * 1024     // 256KB buffered writes
	readBufSize   = 1 * 1024 * 1024 // 1MB buffered reads
)

// CountUniqueIPs: 2-pass exact counting with disk buckets.
// Pass 1: partition into 256 files by top byte.
// Pass 2: per bucket, use a 2MB bitset on the 24-bit suffix.
func CountUniqueIPs(filename string) (int64, error) {
	// --- Pass 1: split into temp files ---
	dir, err := os.MkdirTemp("", "ipbuckets-*")
	if err != nil {
		return 0, fmt.Errorf("mkdtemp: %w", err)
	}
	defer os.RemoveAll(dir)

	files := make([]*os.File, numBuckets)
	writers := make([]*bufio.Writer, numBuckets)
	for i := 0; i < numBuckets; i++ {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("b%03d.bin", i)))
		if err != nil {
			return 0, fmt.Errorf("create bucket: %w", err)
		}
		files[i] = f
		w := bufio.NewWriterSize(f, writeBufSize)
		writers[i] = w
	}
	// Close all writers/files at the end of pass1
	flushClose := func() {
		for i := 0; i < numBuckets; i++ {
			if writers[i] != nil {
				writers[i].Flush()
				writers[i] = nil
			}
			if files[i] != nil {
				files[i].Close()
				files[i] = nil
			}
		}
	}

	src, err := os.Open(filename)
	if err != nil {
		flushClose()
		return 0, fmt.Errorf("open input: %w", err)
	}
	defer src.Close()

	r := bufio.NewReaderSize(src, readBufSize)
	var line []byte
	for {
		b, err := r.ReadBytes('\n')
		if errorsIsEOFOrNil := err == nil || err == io.EOF; !errorsIsEOFOrNil {
			flushClose()
			return 0, fmt.Errorf("read: %w", err)
		}
		// handle last line without '\n'
		if idx := bytes.LastIndexByte(b, '\n'); idx >= 0 {
			line = bytes.TrimSpace(b[:idx])
		} else {
			line = bytes.TrimSpace(b)
		}
		if len(line) != 0 {
			ip, perr := utils.ParseIPv4(line)
			if perr == nil {
				top := byte(ip >> 24)
				suffix := ip & 0x00FFFFFF
				var buf [4]byte
				binary.BigEndian.PutUint32(buf[:], suffix)
				writers[top].Write(buf[:]) // write suffix only (4 bytes)
			}
		}
		if err == io.EOF {
			break
		}
	}
	flushClose()

	// --- Pass 2: for each bucket, count uniques with a 2MB bitset ---
	var total int64
	for i := 0; i < numBuckets; i++ {
		path := filepath.Join(dir, fmt.Sprintf("b%03d.bin", i))
		f, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("open bucket %d: %w", i, err)
		}

		// 2^24 bits → 2MB; store as []uint32 to set bits quickly
		bitset := make([]uint32, wordsPerSet)

		rr := bufio.NewReaderSize(f, readBufSize)
		var buf [4]byte
		var added int64
		for {
			_, err := io.ReadFull(rr, buf[:])
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				// trailing corruption: ignore
				break
			}
			if err != nil {
				f.Close()
				return 0, fmt.Errorf("read bucket %d: %w", i, err)
			}
			suffix := binary.BigEndian.Uint32(buf[:]) // 0..2^24-1
			word := suffix >> 5       // /32
			bit := suffix & 31        // %32
			mask := uint32(1) << bit
			if (bitset[word] & mask) == 0 {
				bitset[word] |= mask
				added++
			}
		}
		f.Close()
		total += added
		// bitset gets GC'd before next bucket; peak RAM stays small
	}

	return total, nil
}
