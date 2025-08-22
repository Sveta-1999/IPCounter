package naive

/*
Package naive provides an implementation for counting unique IPv4 addresses.

It reads a file containing one IPv4 address per line, parses each address into a 32-bit
unsigned integer, and tracks uniqueness using a Go map.

Pros:
- Simple and easy to understand.
- Correctly counts unique IPs for small to medium-sized files.

Cons:
- Uses a map that grows with the number of unique IPs,
  which can consume a large amount of memory for very big files.
- Not suitable for extremely large datasets.
*/

import (
	"ipcounter/utils"
	"bufio"
	"fmt"
	"os"
	"strings"
)

type NaiveCounter struct{}

func New() *NaiveCounter {
	return &NaiveCounter{}
}

func (c *NaiveCounter) CountUniqueIPs(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	uniqueIPs := make(map[uint32]struct{})
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		ipInt, err := utils.IPToUint32(line)
		if err != nil {
			continue
		}
		uniqueIPs[ipInt] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading file: %w", err)
	}

	return int64(len(uniqueIPs)), nil
}