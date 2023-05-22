package queue

import "sync/atomic"

/*
 * SingleWithoutLockQueue is a one producer and one consumer queue
 * without locks.
 */

type queueNode[T any] struct {
	data T
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

type SingleWithoutLockQueue[T any] struct {
	_pad0      [64]byte
	size       uint32
	capMod     uint32
	_pad1      [64]byte
	records    []queueNode[T]
	_pad2      [64]byte
	readIndex  uint32
	_pad3      [64]byte
	writeIndex uint32
}

func NewSingleWithoutLockQueue[T any](capacity uint32) *SingleWithoutLockQueue[T] {
	q := new(SingleWithoutLockQueue[T])
	q.size = RoundUp(capacity)
	q.capMod = q.size - 1
	q.records = make([]queueNode[T], q.size)
	return q
}

func (m *SingleWithoutLockQueue[T]) Write(value T) bool {
	n := &m.records[m.writeIndex]
	if 1 == atomic.LoadUint32(&n.flag) {
		return false
	}
	n.data = value
	n.flag = 1

	m.writeIndex = (m.writeIndex + 1) & m.capMod
	return true
}

func (m *SingleWithoutLockQueue[T]) Read() (ret T, ok bool) {
	n := &m.records[m.readIndex]
	if 0 == atomic.LoadUint32(&n.flag) {
		return ret, false
	}

	ret, n.data = n.data, ret
	n.flag = 0

	m.readIndex = (m.readIndex + 1) & m.capMod
	return ret, true
}

func (m *SingleWithoutLockQueue[T]) IsEmpty() bool {
	n := &m.records[m.readIndex]
	if 0 == atomic.LoadUint32(&n.flag) {
		return true
	}
	return false
}

func (m *SingleWithoutLockQueue[T]) Count() uint32 {
	w := atomic.LoadUint32(&m.writeIndex)
	r := atomic.LoadUint32(&m.readIndex)
	if w >= r {
		return w - r
	} else {
		return m.size - r + w
	}
}

func (m *SingleWithoutLockQueue[T]) Capacity() uint32 {
	return m.size
}
