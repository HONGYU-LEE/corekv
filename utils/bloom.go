// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "math"

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	// only hashCount
	if len(f) < 2 {
		return false
	}

	hashCount := f[len(f)-1]
	// Too many hash functions indicate that the data is no longer valid
	if hashCount > 30 {
		return true
	}

	delta := h>>17 | h<<15
	needBits := uint32(8 * (len(f) - 1))
	for i := 0; i < int(hashCount); i++ {
		bitPos := h % needBits
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}

	return true
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	//Calculate how many bits a key occupies
	bitSize := -1 * float64(numEntries) * math.Log(fp) / math.Pow(math.Log(2), 2)

	return int(math.Ceil(bitSize / float64(numEntries)))
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	hashCount := uint32(float64(bitsPerKey) * math.Log(2))
	if hashCount < 1 {
		hashCount = 1
	} else if hashCount > 30 {
		hashCount = 30
	}

	needBits := bitsPerKey * len(keys)
	if needBits < 64 {
		needBits = 64
	}

	needBytes := (needBits + 7) / 8
	needBits = needBytes * 8

	filter := make([]byte, needBytes+1) // Add a byte to store the hashCount

	// Reference leveldb
	for _, hashCode := range keys {
		delta := hashCode>>17 | hashCode<<15 // Rotate right 17 bits
		//Simulate multiple hashFun by calculating the delta
		for i := 0; i < int(hashCount); i++ {
			bitPos := hashCode % uint32(needBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			hashCode += delta
		}
	}

	filter[needBytes] = uint8(hashCount)

	return filter
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	// Reference Redis's MurmurHash2

	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)

	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}

	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h

	// const (
	// 	seed = 0xbc9f1d34
	// 	m = 0x5bd1e995
	// 	r = 24
	// )

	// h := uint32(seed) ^ uint32(len(b)) * m

	// for i := len(b); i >= 4; i -= 4 {
	// 	k := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24

	// 	k *= m
	// 	k ^= k >> r
	// 	k *= m

	// 	h *= m
	// 	h ^= k

	// 	b = b[4:]
	// }

	// switch len(b) {
	// case 3 :
	// 	h ^= uint32(b[2]) << 16
	// 	fallthrough
	// case 2 :
	// 	h ^= uint32(b[1]) << 8
	// 	fallthrough
	// case 1 :
	// 	h ^= uint32(b[0])
	// 	h *= m
	// }
	// h ^= h >> 13
	// h *= m
	// h ^= h >> 15

	// return h
}
