package concurrent

import (
	"runtime"
	"sync"
	"sync/atomic"
	"bufio"
	"os"
	"fmt"
	"io"
	"bytes"

	"ipcounter/utils"
)

const (
	maxIPv4   = uint64(1) << 32   // total IPv4 space (2^32)
	numShards = 16384             // number of bitset partitions
)

var (
	bitsPerShard  = (maxIPv4 + uint64(numShards) - 1) / uint64(numShards)
	wordsPerShard = (bitsPerShard + 31) / 32 // 32 bits per uint32 word
)

// shard represents a lazily allocated slice of bits
type shard struct {
	once  sync.Once  // ensure words init only once
	words []uint32   // bit array (32 IPs per uint32)
}

func (s *shard) ensure() {
	s.once.Do(func() {
		s.words = make([]uint32, wordsPerShard)
	})
}

// BitsetCounter tracks seen IPs in multiple shards
type BitsetCounter struct {
	shards []*shard
}

// New creates a BitsetCounter with uninitialized shards
func New() *BitsetCounter {
	shards := make([]*shard, numShards)
	for i := range shards {
		shards[i] = &shard{} // lazy init on first write
	}
	return &BitsetCounter{shards: shards}
}

// setBit marks the given bit if not already set, returns true if it was new
func setBit(s *shard, offset uint32) bool {
	s.ensure()
	wordIdx := offset / 32
	bitIdx := offset % 32
	mask := uint32(1) << bitIdx
	ptr := &s.words[wordIdx]

	for {
		old := atomic.LoadUint32(ptr)
		if old&mask != 0 {
			return false // already set
		}
		if atomic.CompareAndSwapUint32(ptr, old, old|mask) {
			return true // successfully set
		}
	}
}

const bytesPerChunk = 2 * 1024 * 1024 // 2 MB read buffer size

// CountUniqueIPs counts distinct IPv4s in a file using concurrent chunk processing
func (b *BitsetCounter) CountUniqueIPs(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	numWorkers := runtime.NumCPU()
	chunkChan := make(chan []byte, numWorkers*2)
	resultChan := make(chan int64, numWorkers*2)

	// Pool for reusing read buffers
	bufPool := sync.Pool{
		New: func() any { return make([]byte, bytesPerChunk) },
	}

	runtime.GOMAXPROCS(numWorkers)

	// Worker goroutines process chunks in parallel
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkChan {
				resultChan <- processChunk(chunk, b)
			}
		}()
	}

	// Aggregator goroutine sums worker results
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	var total int64
	go func() {
		defer resultWg.Done()
		for c := range resultChan {
			total += c
		}
	}()

	// Producer: reads file in chunks, splits at last newline
	var carry []byte
	reader := bufio.NewReader(file)

	for {
		buf := bufPool.Get().([]byte)
		n, readErr := reader.Read(buf)
		if n == 0 && readErr != nil {
			bufPool.Put(buf)
			if readErr == io.EOF {
				break
			}
			return 0, fmt.Errorf("read error: %w", readErr)
		}

		data := buf[:n]
		cut := bytes.LastIndexByte(data, '\n')

		if cut == -1 { // no newline found, accumulate and continue
			carry = append(carry, data...)
			bufPool.Put(buf)
			if readErr == io.EOF {
				break
			}
			continue
		}

		// Complete chunk = carry + data until last newline
		chunk := make([]byte, len(carry)+(cut+1))
		copy(chunk, carry)
		copy(chunk[len(carry):], data[:cut+1])
		carry = carry[:0]

		chunkChan <- chunk

		// Save remainder after last newline
		if cut+1 < len(data) {
			carry = append(carry, data[cut+1:]...)
		}

		bufPool.Put(buf)

		if readErr == io.EOF {
			break
		}
	}

	// Send leftover tail without newline
	if len(carry) > 0 {
		chunk := make([]byte, len(carry))
		copy(chunk, carry)
		chunkChan <- chunk
	}

	// Cleanup
	close(chunkChan)
	wg.Wait()
	close(resultChan)
	resultWg.Wait()

	return total, nil
}

// processChunk parses IPs in chunk and updates bitset
func processChunk(chunk []byte, b *BitsetCounter) int64 {
	var count int64
	start := 0

	for i, c := range chunk {
		if c == '\n' {
			line := bytes.TrimSpace(chunk[start:i])
			start = i + 1
			if len(line) == 0 { continue }

			ipInt, err := utils.ParseIPv4(line)
			if err != nil { continue }

			shardIdx := ipInt % numShards
			if setBit(b.shards[shardIdx], ipInt/numShards) {
				count++
			}
		}
	}

	// Handle last line (no trailing newline)
	if start < len(chunk) {
		line := bytes.TrimSpace(chunk[start:])
		if len(line) > 0 {
			if ipInt, err := utils.ParseIPv4(line); err == nil {
				shardIdx := ipInt % numShards
				if setBit(b.shards[shardIdx], ipInt/numShards) {
					count++
				}
			}
		}
	}
	return count
}
