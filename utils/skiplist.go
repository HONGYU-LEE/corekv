package utils

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel - 1,
		rand:     r,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.RLock()
	defer list.lock.RUnlock()

	score := list.calcScore(data.Key)
	prev := list.header

	prevs := make([]*Element, list.maxLevel+1) //Record the previous of each level

	//Find the pos of the node
	for i := list.maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				//update entry
				if comp == 0 {
					next.entry = data
					return nil
				} else {
					prev = next
				}
			} else {
				break
			}
		}
		prevs[i] = prev
	}

	randLevel := list.randLevel()
	entry := newElement(score, data, randLevel)

	//insert entry
	for i := randLevel; i >= 0; i-- {
		next := prevs[i].levels[i]
		prevs[i].levels[i] = entry
		entry.levels[i] = next
	}

	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	score := list.calcScore(key)
	prev := list.header

	for i := list.maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return next.entry
				} else {
					prev = next
				}
			} else {
				break
			}
		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

//Compare the first eight digits to speed up the query
func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	} else if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	for level := 1; level < list.maxLevel; level++ {
		if RandN(1000)%2 == 0 {
			return level
		}
	}
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	return list.size
}
