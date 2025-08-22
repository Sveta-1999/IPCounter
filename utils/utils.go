package utils

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

func IPToUint32(ipStr string) (uint32, error) {
	ip := net.ParseIP(strings.TrimSpace(ipStr)).To4()
	if ip == nil {
		return 0, fmt.Errorf("invalid IPv4 address: %s", ipStr)
	}

	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3]), nil
}

func ParseIPv4(b []byte) (uint32, error) {
	var ip, part uint32
	pos := 0

	// Octet 1 (1-3 digits)
	if pos >= len(b) {
		return 0, errors.New("too short")
	}
	c := b[pos]
	if c < '0' || c > '9' {
		return 0, errors.New("invalid digit")
	}
	part = uint32(c - '0')
	pos++

	if pos < len(b) {
		c = b[pos]
		if c >= '0' && c <= '9' {
			part = part*10 + uint32(c-'0')
			pos++
			if pos < len(b) {
				c = b[pos]
				if c >= '0' && c <= '9' {
					part = part*10 + uint32(c-'0')
					if part > 255 {
						return 0, errors.New("invalid octet")
					}
					pos++
				}
			}
		}
	}
	ip = part
	if pos >= len(b) || b[pos] != '.' {
		return 0, errors.New("expected dot")
	}
	pos++
	part = 0

	// Octet 2 (1-3 digits)
	if pos >= len(b) {
		return 0, errors.New("too short")
	}
	c = b[pos]
	if c < '0' || c > '9' {
		return 0, errors.New("invalid digit")
	}
	part = uint32(c - '0')
	pos++

	if pos < len(b) {
		c = b[pos]
		if c >= '0' && c <= '9' {
			part = part*10 + uint32(c-'0')
			pos++
			if pos < len(b) {
				c = b[pos]
				if c >= '0' && c <= '9' {
					part = part*10 + uint32(c-'0')
					if part > 255 {
						return 0, errors.New("invalid octet")
					}
					pos++
				}
			}
		}
	}
	ip = (ip << 8) | part
	if pos >= len(b) || b[pos] != '.' {
		return 0, errors.New("expected dot")
	}
	pos++
	part = 0

	// Octet 3 (1-3 digits)
	if pos >= len(b) {
		return 0, errors.New("too short")
	}
	c = b[pos]
	if c < '0' || c > '9' {
		return 0, errors.New("invalid digit")
	}
	part = uint32(c - '0')
	pos++

	if pos < len(b) {
		c = b[pos]
		if c >= '0' && c <= '9' {
			part = part*10 + uint32(c-'0')
			pos++
			if pos < len(b) {
				c = b[pos]
				if c >= '0' && c <= '9' {
					part = part*10 + uint32(c-'0')
					if part > 255 {
						return 0, errors.New("invalid octet")
					}
					pos++
				}
			}
		}
	}
	ip = (ip << 8) | part
	if pos >= len(b) || b[pos] != '.' {
		return 0, errors.New("expected dot")
	}
	pos++
	part = 0

	// Octet 4 (1-3 digits, no trailing dot)
	if pos >= len(b) {
		return 0, errors.New("too short")
	}
	c = b[pos]
	if c < '0' || c > '9' {
		return 0, errors.New("invalid digit")
	}
	part = uint32(c - '0')
	pos++

	if pos < len(b) {
		c = b[pos]
		if c >= '0' && c <= '9' {
			part = part*10 + uint32(c-'0')
			pos++
			if pos < len(b) {
				c = b[pos]
				if c >= '0' && c <= '9' {
					part = part*10 + uint32(c-'0')
					if part > 255 {
						return 0, errors.New("invalid octet")
					}
					pos++
				}
			}
		}
	}
	if pos != len(b) {
		return 0, errors.New("extra data")
	}
	return (ip << 8) | part, nil
}