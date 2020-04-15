package queue

import "sync/atomic"

/*
 * SingleWithoutLockQueue is a one producer and one consumer queue
 * without locks.
 */

type queueNode struct {
	data interface{}
	flag uint32
}

func RoundUp(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

type SingleWithoutLockQueue struct {
	_pad0      [64]byte
	size       uint32
	capMod     uint32
	_pad1      [64]byte
	records    []*queueNode
	_pad2      [64]byte
	readIndex  uint32
	_pad3      [64]byte
	writeIndex uint32
}

func NewSingleWithoutLockQueue(capacity uint32) *SingleWithoutLockQueue {
	q := new(SingleWithoutLockQueue)
	q.size = RoundUp(capacity)
	q.capMod = q.size - 1
	q.records = make([]*queueNode, q.size)
	for i := uint32(0); i < q.size; i++ {
		q.records[i] = &queueNode{}
	}
	return q
}

func (m *SingleWithoutLockQueue) Write(value interface{}) bool {
	n := m.records[m.writeIndex]
	if 1 == atomic.LoadUint32(&n.flag) {
		return false
	}
	n.data = value
	n.flag = 1

	m.writeIndex = (m.writeIndex + 1) & m.capMod
	return true
}

func (m *SingleWithoutLockQueue) Read() (v interface{}, b bool) {
	n := m.records[m.readIndex]
	if 0 == atomic.LoadUint32(&n.flag) {
		return nil, false
	}

	v = n.data
	n.data = nil
	n.flag = 0

	m.readIndex = (m.readIndex + 1) & m.capMod
	return v, true
}

func (m *SingleWithoutLockQueue) IsEmpty() bool {
	n := m.records[m.readIndex]
	if 0 == atomic.LoadUint32(&n.flag) {
		return true
	}
	return false
}

func (m *SingleWithoutLockQueue) Count() uint32 {
	w := atomic.LoadUint32(&m.writeIndex)
	r := atomic.LoadUint32(&m.readIndex)
	if w >= r {
		return w - r
	} else {
		return m.size - r + w
	}
}

func (m *SingleWithoutLockQueue) Capacity() uint32 {
	return m.size
}
