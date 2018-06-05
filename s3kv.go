// Package s3kv provides simple effective key value storage for s3.
package s3kv

import (
	"bytes"
	"crypto/md5"
	"strconv"
	"strings"
)

type (
	// Segment models a key / path segment, which may be joined head and / or tail to other(s), with an optional
	// separator, and has a flag indicating if the key will be written as is (default) or if set to true, the key
	// will be written as a hexadecimal formatted hash.
	Segment struct {
		Key  []byte
		Hash bool
	}

	// Config models the full set of configuration available to this service.
	Config struct {
		*HashConfig
	}

	// HashConfig models the specific configuration for hashing of segments.
	HashConfig struct {
		// Separator is the segment separator.
		Separator []byte

		// Max is the max hash number, the max number of combinations for each hash prefix will be max + 1, note that
		// hashes will get converted into padded hexadecimal (lowercase).
		Max uint32

		// HashFunc is the function that will scramble the segments to be hashed.
		HashFunc func([]byte) []byte
	}

	HashOption func(config *HashConfig)
)

// DefaultConfig returns the default configuration for this package.
func DefaultConfig() *Config {
	return &Config{
		HashConfig: &HashConfig{
			Separator: []byte(`/`),
			Max:       4095,
			HashFunc:  HashMD5,
		},
	}
}

// Hash calls the method of the same name on the DefaultConfig.
func Hash(path []Segment, opts ... HashOption) []byte {
	return DefaultConfig().HashConfig.Hash(path, opts...)
}

// Hash converts a slice of Segment into a path, joining (imploding) segments using c.Separator, and writing either
// the raw Segment.Key (if Segment.Hash is false) OR the hashed representation of Segment.Key, generated using the
// HashConfig.Max and HashConfig.HashFunc properties as additional input.
func (c HashConfig) Hash(path []Segment, opts ... HashOption) []byte {
	c.Apply(opts...)
	buffer := new(bytes.Buffer)
	for i, seg := range path {
		if i != 0 {
			buffer.Write(c.Separator)
		}
		if !seg.Hash {
			buffer.Write(seg.Key)
			continue
		}
		buffer.WriteString(
			PaddedHex(
				ReduceBytes(c.HashFunc(seg.Key)),
				c.Max,
			),
		)
	}
	return buffer.Bytes()
}

// Apply modifies the HashConfig by applying the opts.
func (c *HashConfig) Apply(opts ... HashOption) {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(c)
	}
}

// HashMD5 returns the md5 sum of b as a slice.
func HashMD5(b []byte) []byte {
	result := md5.Sum(b)
	return result[:]
}

// ReduceBytes takes b % uint32(max).
func ReduceBytes(b []byte) uint32 {
	var m uint32
	m--
	var r uint64 = 0
	for i := range b {
		r *= 256 % (uint64(m) + 1)
		r %= uint64(m) + 1
		r += uint64(b[i]) % (uint64(m) + 1)
		r %= uint64(m) + 1
	}
	return uint32(r)
}

// PaddedHex converts value to a hexadecimal representation of at most max+1 COMBINATIONS, with `0` left padding
// based on the max, basically you want to use max values like 15, 255, 4095, etc.
func PaddedHex(value, max uint32) (result string) {
	m := uint64(max) + 1
	result = strconv.FormatUint(
		uint64(value)%m,
		16,
	)
	l := 1
	for ; m > 16; m = (m / 16) + (m % 16) {
		l++
	}
	l -= len(result)
	if l > 0 {
		result = strings.Repeat(`0`, l) + result
	}
	return
}
