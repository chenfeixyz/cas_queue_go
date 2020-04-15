package queue

import (
	"runtime"
	"sync/atomic"
)

/*
 * cas锁 多线程安全
 */

type MulitWithoutLockQueue struct {
	_pad0      [64]byte // false shareing pad
	size       uint32
	capMod     uint32
	_pad1      [64]byte
	records    []*queueNode
	_pad2      [64]byte
	readIndex  uint32
	_pad3      [64]byte
	writeIndex uint32
}

func NewMulitWithoutLockQueue(capaciity uint32) *MulitWithoutLockQueue {
	q := new(MulitWithoutLockQueue)
	q.size = RoundUp(capaciity)
	q.capMod = q.size - 1
	q.records = make([]*queueNode, q.size)
	for i := uint32(0); i < q.size; i++ {
		q.records[i] = &queueNode{}
	}
	return q
}

func (m *MulitWithoutLockQueue) Write(value interface{}) bool {
	for {
		w := atomic.LoadUint32(&m.writeIndex)
		n := m.records[w]
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

func (m *MulitWithoutLockQueue) Read() (interface{}, bool) {
	for {
		r := atomic.LoadUint32(&m.readIndex)
		n := m.records[r]
		if 0 == atomic.LoadUint32(&n.flag) {
			return nil, false
		}

		if atomic.CompareAndSwapUint32(&m.readIndex, r, (r+1)&m.capMod) {
			d := n.data
			n.data = nil
			n.flag = 0
			return d, true
		}
		runtime.Gosched()
	}
}

func (m *MulitWithoutLockQueue) Count() uint32 {
	w := atomic.LoadUint32(&m.writeIndex)
	r := atomic.LoadUint32(&m.readIndex)
	if w >= r {
		return w - r
	} else {
		return m.size - r + w
	}
}

func (m *MulitWithoutLockQueue) Capacity() uint32 {
	return m.size
}
