// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package semaphore

import (
	"context"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const maxSleep = 1 * time.Millisecond

func HammerWeighted(sem Weighted, n int64, loops int) {
	for i := 0; i < loops; i++ {
		sem.Acquire(context.Background(), n)
		time.Sleep(time.Duration(rand.Int63n(int64(maxSleep/time.Nanosecond))) * time.Nanosecond)
		sem.Release(n)
	}
}

func TestWeighted(t *testing.T) {
	t.Parallel()

	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n
	sem := *NewWeighted(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			HammerWeighted(sem, int64(i), loops)
		}()
	}
	wg.Wait()
}

func TestWeightedPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("release of an unacquired weighted semaphore did not panic")
		}
	}()
	w := NewWeighted(1)
	w.Release(1)
}

func TestWeightedTryAcquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := NewWeighted(2)
	tries := []bool{}
	sem.Acquire(ctx, 1)
	tries = append(tries, sem.TryAcquire(1))
	tries = append(tries, sem.TryAcquire(1))

	sem.Release(2)

	tries = append(tries, sem.TryAcquire(1))
	sem.Acquire(ctx, 1)
	tries = append(tries, sem.TryAcquire(1))

	want := []bool{true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestWeightedAcquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := NewWeighted(2)
	tryAcquire := func(n int64) bool {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		return sem.Acquire(ctx, n) == nil
	}

	tries := []bool{}
	sem.Acquire(ctx, 1)
	tries = append(tries, tryAcquire(1))
	tries = append(tries, tryAcquire(1))

	sem.Release(2)

	tries = append(tries, tryAcquire(1))
	sem.Acquire(ctx, 1)
	tries = append(tries, tryAcquire(1))

	want := []bool{true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestWeightedDoesntBlockIfTooBig(t *testing.T) {
	t.Parallel()

	const n = 2
	sem := NewWeighted(n)
	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go sem.Acquire(ctx, n+1)
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := n * 3; i > 0; i-- {
		g.Go(func() error {
			err := sem.Acquire(ctx, 1)
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				sem.Release(1)
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		t.Errorf("semaphore.NewWeighted(%v) failed to AcquireCtx(_, 1) with AcquireCtx(_, %v) pending", n, n+1)
	}
}

// TestLargeAcquireDoesntStarve times out if a large call to Acquire starves.
// Merely returning from the test function indicates success.
func TestLargeAcquireDoesntStarve(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	n := int64(runtime.GOMAXPROCS(0))
	sem := NewWeighted(n)
	running := true

	var wg sync.WaitGroup
	wg.Add(int(n))
	for i := n; i > 0; i-- {
		sem.Acquire(ctx, 1)
		go func() {
			defer func() {
				sem.Release(1)
				wg.Done()
			}()
			for running {
				time.Sleep(1 * time.Millisecond)
				sem.Release(1)
				sem.Acquire(ctx, 1)
			}
		}()
	}

	sem.Acquire(ctx, n)
	running = false
	sem.Release(n)
	wg.Wait()
}

func TestWeightedResizePanic(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("release of an unacquired weighted semaphore did not panic")
		}
	}()
	w := NewWeighted(1)
	w.Resize(-1)
}

func TestWeightedResize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sem := NewWeighted(3)
	tryAcquire := func(n int64) bool {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		return sem.Acquire(ctx, n) == nil
	}

	tries := []bool{}

	tries = append(tries, tryAcquire(1)) // true;  cur/size = 1/3
	tries = append(tries, tryAcquire(1)) // true;  cur/size = 2/3
	tries = append(tries, tryAcquire(1)) // true;  cur/size = 3/3
	tries = append(tries, tryAcquire(1)) // false; cur/size = 3/3 - full!

	sem.Resize(2) // cur/size = 3/2

	tries = append(tries, tryAcquire(1)) // false; cur/size = 3/3 - full!

	sem.Release(1) // cur/size = 2/2

	tries = append(tries, tryAcquire(1)) // false; cur/size = 2/2 - full!

	sem.Release(1) // cur/size = 1/2

	tries = append(tries, tryAcquire(1)) // true;  cur/size = 2/2

	tries = append(tries, tryAcquire(1)) // false; cur/size = 2/2 - full!

	sem.Resize(3) // cur/size = 2/3

	tries = append(tries, tryAcquire(1)) // true;  cur/size = 3/3

	tries = append(tries, tryAcquire(1)) // false; cur/size = 3/3 - full!

	want := []bool{true, true, true, false, false, false, true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

// TestWeightedResizeUnblockImpossible will fail if an acquire that was blocking because of an impossible weight was
// still blocking after resize to bigger size OR if an acquire that was possible but isn't possible after resize is
// blocking because it is still in waiters list.
// Merely returning from the test function indicates success.
func TestWeightedResizeUnblockImpossible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sem := NewWeighted(3)

	acquire := func(n int64, syncChan chan struct{}) chan struct{} {
		signal := make(chan struct{})
		go func() {
			syncChan <- struct{}{}
			sem.Acquire(ctx, n)
			defer sem.Release(n)
			signal <- struct{}{}
			close(signal)
		}()
		return signal
	}

	// Use this syncChan to make sure acquire()  get blocked in sequence (to have internal linked-list in desired state)
	syncChan := make(chan struct{})

	doneAcquire5 := acquire(5, syncChan)
	<-syncChan
	doneAcquire4 := acquire(4, syncChan)
	<-syncChan

	select {
	case <-doneAcquire5:
		t.Errorf("An Impossible acquire was aqcuired")
	case <-doneAcquire4:
		t.Errorf("An Impossible acquire was aqcuired")
	default:

	}

	sem.Resize(4)

	select {
	case <-doneAcquire5:
		t.Errorf("An Impossible acquire was aqcuired")
	case <-doneAcquire4:

	}

	sem.Resize(5)

	select {
	case <-doneAcquire5:

	}

	sem.Acquire(ctx, 1)

	// Test down-sizing the semaphore with to-be-impossible waiters in waiting.
	syncChan = make(chan struct{})
	doneAcquire5 = acquire(5, syncChan)
	<-syncChan
	doneAcquire4 = acquire(4, syncChan)
	<-syncChan

	sem.Resize(4)

	sem.Release(1)

	select {
	case <-doneAcquire5:
		t.Errorf("An Impossible acquire was aqcuired")
	case <-doneAcquire4:

	}

}

func TestGetters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	type test struct{ curr, size, waiters int }

	var tries []test

	sem := NewWeighted(3)

	sem.Acquire(ctx, 1)
	sem.Acquire(ctx, 1)

	tries = append(tries, test{curr: int(sem.Current()), size: int(sem.Size()), waiters: sem.Waiters()})

	sem.Acquire(ctx, 1)
	go sem.Acquire(ctx, 1)

	time.Sleep(100 * time.Millisecond)

	tries = append(tries, test{curr: int(sem.Current()), size: int(sem.Size()), waiters: sem.Waiters()})

	want := []test{{2, 3, 0}, {3, 3, 1}}

	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %+v, want %+v", i, tries[i], want[i])
		}
	}
}
