// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package semaphore provides a weighted semaphore implementation.
package semaphore

import (
	"container/list"
	"context"
	"sync"
)



type waiter struct {
	n     int64
	ready chan<- struct{} // Closed when semaphore acquired.
}

// NewWeighted creates a new weighted semaphore with the given
// maximum combined weight for concurrent access.
func NewWeighted(n int64) *Weighted {
	w := &Weighted{size: n}
	return w
}

// Weighted provides a way to bound concurrent access to a resource.
// The callers can request access with a given weight.
//
//
//
//
type Weighted struct {
	size              int64			// 总量
	cur               int64			// 当前占有量，size - cur 为空闲量
	mu                sync.Mutex	//
	waiters           list.List		//
	impossibleWaiters list.List		//
}

// Acquire acquires the semaphore with a weight of n, blocking until resources
// are available or ctx is done. On success, returns nil. On failure, returns
// ctx.Err() and leaves the semaphore unchanged.
//
// If ctx is already done, Acquire may still succeed without blocking.
//
//
//
func (s *Weighted) Acquire(ctx context.Context, n int64) error {

	s.mu.Lock() // 加锁

	// 若当前有可用资源，且未有等待者，则直接分配资源并返回。
	if s.size-s.cur >= n && s.waiters.Len() == 0 {
		// 占用 n 个资源
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	// 若当前无足够资源，或者当前有等待者在等待，则必须按序分配资源(避免饥饿)，将当前 waiter 先入队等待。

	// 如果当前申请资源量超过总量，则添加 waiter 到 impossibleWaiters 队列中，否则添加到 waiters 中。
	// 正常的资源释放只会检查 waiters 队列，仅当资源扩缩容时，才会检查 impossibleWaiters 队列。
	var waiterList = &s.waiters
	if n > s.size {
		// Add doomed Acquire call to the Impossible waiters list.
		waiterList = &s.impossibleWaiters
	}

	// 构造 waiter 对象
	ready := make(chan struct{})
	w := waiter{
		n: n,
		ready: ready,
	}

	// 将 waiter 插入到 waiters/impossibleWaiters 队列中
	elem := waiterList.PushBack(w)
	s.mu.Unlock() // 释放锁


	// 等待资源申请完成 或者 ctx 被 cancel(timeout) 。
	//
	// 注意，因为此前已经释放了锁，所以资源释放操作可能正在进行中，而 select 执行 case 的顺序是随机的，
	// 所以存在一种可能，在 ctx.Done 触发时，已经完成了资源分配和 close(ready) 通知操作(概率较小)。
	//
	// 所以，在 case ctx.Done 执行时，需要检查是否完成资源分配，若已分配则无需重复移除队列并报错，避免
	// 造成资源泄漏。
	//
	select {
	case <-ctx.Done():
		err := ctx.Err()
		// 这里的锁很关键，避免对 waiterList/impossibleWaiters 的并发操作。
		s.mu.Lock()
		select {
		// 检查是否已经完成分配，未分配则从等待队列中移除。
		case <-ready:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancelation.
			err = nil
		default:
			waiterList.Remove(elem)
		}
		s.mu.Unlock()
		return err
	// 分配成功，直接返回。
	case <-ready:
		return nil
	}
}

// TryAcquire acquires the semaphore with a weight of n without blocking.
// On success, returns true. On failure, returns false and leaves the semaphore unchanged.
//
// 非阻塞式申请
func (s *Weighted) TryAcquire(n int64) bool {
	s.mu.Lock()
	success := s.size-s.cur >= n && s.waiters.Len() == 0
	if success {
		s.cur += n
	}
	s.mu.Unlock()
	return success
}

// Release releases the semaphore with a weight of n.
//
// 资源释放
func (s *Weighted) Release(n int64) {
	s.mu.Lock()

	// 解除资源占用
	s.cur -= n

	// 异常检查
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: bad release")
	}

	// 遍历 waiters 队列，按照 FIFO 方式检查资源是否可满足。
	for {

		// 取首元素
		next := s.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		// 类型转换
		w := next.Value.(waiter)

		// 资源检查
		if s.size-s.cur < w.n {
			// Not enough tokens for the next waiter.  We could keep going (to try to
			// find a waiter with a smaller request), but under load that could cause
			// starvation for large requests; instead, we leave all remaining waiters
			// blocked.
			//
			// Consider a semaphore used as a read-write lock, with N tokens, N
			// readers, and one writer.  Each reader can Acquire(1) to obtain a read
			// lock.  The writer can Acquire(N) to obtain a write lock, excluding all
			// of the readers.  If we allow the readers to jump ahead in the queue,
			// the writer will starve — there is always one token available for every
			// reader.
			//
			// 由于 waiters 队列未按资源申请量排序，按理应该继续向后检查，找到可分配的 waiter 。
			// 但是这样对于大量资源的请求者，会发生饥饿的情况，所以按照 FIFO 的方式来分配。
			//
			// 假设把 semaphore 当作读写锁使用，资源总数设为 N ，一写多读场景下：
			// 读者通过 Acquire(1) 获取读锁，则同时允许 N 个读者并发读，写者通过 Acquire(N) 获取写锁，
			// 会实现读写互斥。这种场景下，如果不按照 FIFO 分配方式，则写者会发生饥饿，因为每个读者
			// 申请的资源量都比较小，容易被满足。
			break
		}

		// 资源分配
		s.cur += w.n

		// 移除 waiter
		s.waiters.Remove(next)

		// 主动通知，解除阻塞
		close(w.ready) // 注意，在 Acquire 中会检查此管道状态
	}


	s.mu.Unlock()
}

// Resize semaphore.
func (s *Weighted) Resize(n int64) {
	s.mu.Lock()

	// 更改资源量
	s.size = n

	// 异常检查
	if s.size < 0 {
		s.mu.Unlock()
		panic("semaphore: bad resize")
	}

	// Add the now possible waiters to waiters list.
	//
	// 在扩/缩容时，才会检查 impossibleWaiters 队列，将满足申请条件的移入到 waiter 队列中，注意是追加到队尾。
	element := s.impossibleWaiters.Front()
	for {
		if element == nil {
			break // No more impossible waiters blocked.
		}

		w := element.Value.(waiter)
		// 如果当前 waiter 申请资源仍旧超过 size，则忽略，让其仍保存在 impossibleWaiters 队列中。
		if s.size < w.n {
			// Still Impossible. next.
			element = element.Next()
			continue
		}
		// 否则，将其从 impossibleWaiters 队列移入到 waiter 队列中。
		s.waiters.PushBack(w)
		toRemove := element
		element = element.Next()
		s.impossibleWaiters.Remove(toRemove)

	}


	// Add the now impossible-waiters to impossible waiters list.
	//
	// 在扩/缩容时，检查 waiter 队列，将不满足申请条件的 waiter 从 waiters 队列移入到 impossibleWaiters 队列中。
	element = s.waiters.Front()
	for {
		if element == nil {
			break // No more waiters.
		}

		w := element.Value.(waiter)
		if s.size >= w.n {
			// Still Possible. next.
			element = element.Next()
			continue
		}

		s.impossibleWaiters.PushBack(w)
		toRemove := element
		element = element.Next()
		s.waiters.Remove(toRemove)
	}


	// Release Possible Waiters
	//
	// 在扩/缩容时，检查 waiter 队列，为满足申请条件的 waiter 分配资源并解除阻塞。
	for {
		next := s.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(waiter)
		if s.size-s.cur < w.n {
			// Not enough tokens for the element waiter.  We could keep going (to try to
			// find a waiter with a smaller request), but under load that could cause
			// starvation for large requests; instead, we leave all remaining waiters
			// blocked.
			break
		}

		s.cur += w.n
		s.waiters.Remove(next)
		close(w.ready)
	}
	s.mu.Unlock()
}

// Current returns the current size of semaphore.
// Returned value may instantly change after/during call. use for diagnostic and health-checking only.
func (s *Weighted) Current() int64 {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	return cur
}

// Size returns the maximum size of semaphore.
// Returned value may instantly change after/during call. use for diagnostic and health-checking only.
func (s *Weighted) Size() int64 {
	s.mu.Lock()
	size := s.size
	s.mu.Unlock()
	return size
}

// Waiters returns the number of currently waiting Acquire calls.
// Returned value may instantly change after/during call. use for diagnostic and health-checking only.
func (s *Weighted) Waiters() int {
	s.mu.Lock()
	waiters := s.waiters.Len() + s.impossibleWaiters.Len()
	s.mu.Unlock()
	return waiters
}
