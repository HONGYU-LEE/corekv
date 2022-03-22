package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

type Arena struct {
	n   uint32 //offset
	buf []byte
}

const MaxNodeSize = int(unsafe.Sizeof(Element{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	// 在 arena 中分配指定大小的内存空间
	offset := atomic.AddUint32(&s.n, sz)

	// 空间不足，此时需要扩容
	if len(s.buf) < int(offset)+MaxNodeSize {
		addSize := uint32(len(s.buf))

		// 如果空间过大，则将申请空间置为申请上限
		if addSize > 1<<30 {
			addSize = 1 << 30
		}

		// 如果原空间小于需求空间，则直接增加对应的空间
		if addSize < sz {
			addSize = sz
		}

		//申请新空间，将原数据拷贝到新空间中
		newBuf := make([]byte, len(s.buf)+int(addSize))
		AssertTrue(copy(newBuf, s.buf) == len(s.buf))
		s.buf = newBuf
	}

	return offset - sz
}

//在arena里开辟一块空间，用以存放sl中的节点
//返回值为在arena中的offset
func (s *Arena) putNode(height int) uint32 {
	unNeedSize := (defaultMaxLevel - height) * offsetSize

	needSize := MaxNodeSize - unNeedSize + nodeAlign

	unAlignOffset := s.allocate(uint32(needSize))

	// 进行内存对齐
	alignOffset := (unAlignOffset + uint32(nodeAlign)) & ^uint32(nodeAlign)

	return alignOffset
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	size := v.EncodedSize()

	offset := s.allocate(size)

	v.EncodeValue(s.buf[offset:])

	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	size := uint32(len(key))
	offset := s.allocate(size)

	buf := s.buf[offset : offset+size]
	AssertTrue(copy(buf, key) == len(key))

	return offset
}

func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}

	return (*Element)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(s.buf[offset : offset+size])
	return
}

//用element在内存中的地址 - arena首字节的内存地址，得到在arena中的偏移量
func (s *Arena) getElementOffset(nd *Element) uint32 {
	//获取某个节点，在 arena 当中的偏移量
	if nd == nil {
		return 0
	}

	// 使用Element地址减去buf的起始地址得到偏移量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	// 这个方法用来计算节点在 h 层数下的 next 节点
	return atomic.LoadUint32(&e.levels[h])
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
