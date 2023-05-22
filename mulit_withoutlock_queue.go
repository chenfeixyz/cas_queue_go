package queue

import (
	"runtime"
	"sync/atomic"
)

/*
 * cas锁 多线程安全
 */

type MulitWithoutLockQueue[T any] struct {
	_pad0      [64]byte // false shareing pad
	size       uint32
	capMod     uint32
	_pad1      [64]byte
	records    []queueNode[T]
	_pad2      [64]byte
	readIndex  uint32
	_pad3      [64]byte
	writeIndex uint32
}

func NewMulitWithoutLockQueue[T any](capaciity uint32) *MulitWithoutLockQueue[T] {
	q := new(MulitWithoutLockQueue[T])
	q.size = RoundUp(capaciity)
	q.capMod = q.size - 1
	q.records = make([]queueNode[T], q.size)
	return q
}

func (m *MulitWithoutLockQueue[T]) Write(value T) bool {
	for {
		w := atomic.LoadUint32(&m.writeIndex)
		n := &m.records[w]
		if 1 == atomic.LoadUint32(&n.flag) {
			return false
		}

		if atomic.CompareAndSwapUint32(&m.writeIndex, w, (w+1)&m.capMod) {
			n.data = value
			n.flag = 1
			return true
		}
		runtime.Gosched()
	}
}

func (m *MulitWithoutLockQueue[T]) Read() (ret T, ok bool) {
	for {
		r := atomic.LoadUint32(&m.readIndex)
		n := &m.records[r]
		if 0 == atomic.LoadUint32(&n.flag) {
			return ret, false
		}

		if atomic.CompareAndSwapUint32(&m.readIndex, r, (r+1)&m.capMod) {
			ret, n.data = n.data, ret
			n.flag = 0
			return ret, true
		}
		runtime.Gosched()
	}
}

func (m *MulitWithoutLockQueue[T]) Count() uint32 {
	w := atomic.LoadUint32(&m.writeIndex)
	r := atomic.LoadUint32(&m.readIndex)
	if w >= r {
		return w - r
	} else {
		return m.size - r + w
	}
}

func (m *MulitWithoutLockQueue[T]) Capacity() uint32 {
	return m.size
}
