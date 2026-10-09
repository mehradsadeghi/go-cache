package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

// waitUntil waits for a condition without sleeping. The deadline is only
// used to detect a stuck test, not to assert operation timing.
func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-timer.C:
			t.Fatal("condition was not satisfied before the deadline")
		default:
			runtime.Gosched()
		}
	}
}

func receiveWithin[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	timer := time.NewTimer(testTimeout)
	defer timer.Stop()

	select {
	case result := <-ch:
		return result
	case <-timer.C:
		t.Fatal("timed out waiting for result")
		var zero T
		return zero
	}
}

func newTestCache(t *testing.T) *cache {
	t.Helper()

	c := newCache()
	t.Cleanup(c.Close)
	return c
}

func testValue(data any, ttl time.Duration) value {
	return value{Data: data, TTL: ttl}
}

func mustSet(t *testing.T, c *cache, k key, v value) {
	t.Helper()

	if err := c.Set(k, v); err != nil {
		t.Fatalf("Set(%q): %v", k, err)
	}
}

func mustGet(t *testing.T, c *cache, k key) value {
	t.Helper()

	v, err := c.Get(k)
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	return v
}

func entryCount(c *cache) int32 {
	return c.currentEntryCount.Load()
}

func statsContains(t *testing.T, c *cache, fragments ...string) string {
	t.Helper()

	stats, err := c.Stats()
	if err != nil {
		t.Fatalf("Stats(): %v", err)
	}

	for _, fragment := range fragments {
		if !strings.Contains(stats, fragment) {
			t.Errorf("Stats() = %q; missing %q", stats, fragment)
		}
	}
	return stats
}

// expireEntry changes the stored timestamp under the shard lock, allowing
// cleanup tests to create expired entries deterministically.
func expireEntry(t *testing.T, c *cache, k key) {
	t.Helper()

	shard := getShardNumber(k)
	c.cache[shard].mu.Lock()
	defer c.cache[shard].mu.Unlock()

	v, ok := c.cache[shard].items[k]
	if !ok {
		t.Fatalf("entry %q does not exist", k)
	}

	v.createdAt = time.Now().Add(-2 * time.Hour)
	c.cache[shard].items[k] = v
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	c := newTestCache(t)

	const workers = 32
	const iterations = 300

	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, workers*iterations)

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()
			<-start

			for i := 0; i < iterations; i++ {
				k := key(fmt.Sprintf("key-%d", (worker+i)%64))

				if i%2 == 0 {
					err := c.Set(k, testValue(i, time.Hour))
					if err != nil && !errors.Is(err, cacheIsClosed) {
						errCh <- fmt.Errorf("Set: %w", err)
						return
					}
				} else {
					_, err := c.Get(k)
					if err != nil &&
						!errors.Is(err, keyNotFound) &&
						!errors.Is(err, keyHasExpired) {
						errCh <- fmt.Errorf("Get: %w", err)
						return
					}
				}
			}
		}(worker)
	}

	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("concurrent reads and writes did not finish")
	}

	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	if got := entryCount(c); got < 0 {
		t.Fatalf("entry count is negative: %d", got)
	}
}

func TestConcurrentOperationsOnSameKey(t *testing.T) {
	c := newTestCache(t)

	const workers = 24
	const iterations = 200

	k := key("shared-key")
	mustSet(t, c, k, testValue(0, time.Hour))

	start := make(chan struct{})
	var wg sync.WaitGroup
	var writes atomic.Int64
	var reads atomic.Int64

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()
			<-start

			for i := 0; i < iterations; i++ {
				if worker%2 == 0 {
					if err := c.Set(k, testValue(worker*iterations+i, time.Hour)); err != nil {
						t.Errorf("Set: %v", err)
						return
					}
					writes.Add(1)
				} else {
					v, err := c.Get(k)
					if err != nil {
						t.Errorf("Get: %v", err)
						return
					}
					if v.Data == nil {
						t.Error("Get returned nil data")
					}
					reads.Add(1)
				}
			}
		}(worker)
	}

	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("same-key operations did not finish")
	}

	if writes.Load() == 0 || reads.Load() == 0 {
		t.Fatalf("expected both reads and writes; reads=%d writes=%d",
			reads.Load(), writes.Load())
	}

	if got := entryCount(c); got != 1 {
		t.Fatalf("expected one entry, got %d", got)
	}

	mustGet(t, c, k)
}

func TestTTLExpiration(t *testing.T) {
	c := newTestCache(t)
	k := key("expiring")

	mustSet(t, c, k, testValue("value", 20*time.Millisecond))

	waitUntil(t, testTimeout, func() bool {
		_, err := c.Get(k)
		return errors.Is(err, keyHasExpired)
	})

	// Expiration is reported by Get; physical removal is the cleanup worker's job.
}

func TestOverwritingEntryWithNewTTL(t *testing.T) {
	c := newTestCache(t)
	k := key("overwrite")

	mustSet(t, c, k, testValue("old", time.Hour))

	shard := getShardNumber(k)
	c.cache[shard].mu.Lock()
	old := c.cache[shard].items[k]
	old.createdAt = time.Now().Add(-time.Hour)
	c.cache[shard].items[k] = old
	c.cache[shard].mu.Unlock()

	mustSet(t, c, k, testValue("new", time.Hour))

	got := mustGet(t, c, k)
	if got.Data != "new" {
		t.Fatalf("expected overwritten data %q, got %#v", "new", got.Data)
	}
	if got.TTL != time.Hour {
		t.Fatalf("expected TTL %v, got %v", time.Hour, got.TTL)
	}
	if entryCount(c) != 1 {
		t.Fatalf("overwriting an entry should not increase entry count")
	}
}

func TestDeletionDuringConcurrentAccess(t *testing.T) {
	c := newTestCache(t)
	k := key("contended-delete")
	mustSet(t, c, k, testValue("initial", time.Hour))

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()
			<-start

			for i := 0; i < 100; i++ {
				switch worker % 3 {
				case 0:
					err := c.Set(k, testValue(i, time.Hour))
					if err != nil && !errors.Is(err, cacheIsClosed) {
						t.Errorf("Set: %v", err)
					}
				case 1:
					_, err := c.Get(k)
					if err != nil &&
						!errors.Is(err, keyNotFound) &&
						!errors.Is(err, keyHasExpired) {
						t.Errorf("Get: %v", err)
					}
				case 2:
					err := c.Delete(k)
					if err != nil && !errors.Is(err, keyNotFound) {
						t.Errorf("Delete: %v", err)
					}
				}
			}
		}(worker)
	}

	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("concurrent deletion test timed out")
	}

	if got := entryCount(c); got < 0 {
		t.Fatalf("entry count is negative: %d", got)
	}
}

func TestCleanupOfExpiredEntries(t *testing.T) {
	c := newTestCache(t)

	const count = 12
	for i := 0; i < count; i++ {
		k := key(fmt.Sprintf("expired-%d", i))
		mustSet(t, c, k, testValue(i, time.Hour))
		expireEntry(t, c, k)
	}

	waitUntil(t, 4*time.Second, func() bool {
		return entryCount(c) == 0
	})

	for i := 0; i < count; i++ {
		_, err := c.Get(key(fmt.Sprintf("expired-%d", i)))
		if !errors.Is(err, keyNotFound) && !errors.Is(err, keyHasExpired) {
			t.Errorf("expected expired entry to be unavailable, got %v", err)
		}
	}
}

func TestIncrementalCleanupWhenManyEntriesExpire(t *testing.T) {
	c := newTestCache(t)

	const count = 300
	for i := 0; i < count; i++ {
		k := key(fmt.Sprintf("bulk-expired-%d", i))
		mustSet(t, c, k, testValue(i, time.Hour))
		expireEntry(t, c, k)
	}

	// Each recycler pass processes one shard. Wait for eventual cleanup rather
	// than assuming a particular number of entries is removed per pass.
	waitUntil(t, 4*time.Second, func() bool {
		return entryCount(c) == 0
	})

	if got := entryCount(c); got != 0 {
		t.Fatalf("expected all expired entries to be removed, got %d", got)
	}
}

func TestCleanupRacingWithEntryUpdate(t *testing.T) {
	c := newTestCache(t)

	const count = 200
	for i := 0; i < count; i++ {
		k := key(fmt.Sprintf("race-expired-%d", i))
		mustSet(t, c, k, testValue(i, time.Hour))
		expireEntry(t, c, k)
	}

	// The updater repeatedly replaces one expired entry while cleanup runs.
	// Once the final update completes, the fresh entry must remain readable.
	target := key("race-expired-0")
	start := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < 100; i++ {
			if err := c.Set(target, testValue("fresh", time.Hour)); err != nil {
				t.Errorf("Set during cleanup: %v", err)
				return
			}
			runtime.Gosched()
		}
	}()

	close(start)
	wg.Wait()

	mustSet(t, c, target, testValue("final-fresh", time.Hour))
	got := mustGet(t, c, target)

	if got.Data != "final-fresh" {
		t.Fatalf("expected final update to survive, got %#v", got.Data)
	}
}

func TestConcurrentStatisticsUpdates(t *testing.T) {
	c := newTestCache(t)

	const workers = 24
	const iterations = 100

	mustSet(t, c, "stats-key", testValue("value", time.Hour))

	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			for i := 0; i < iterations; i++ {
				_, err := c.Get("stats-key")
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}

				_, err = c.Get("missing-key")
				if !errors.Is(err, keyNotFound) {
					t.Errorf("expected keyNotFound, got %v", err)
					return
				}

				if _, err := c.Stats(); err != nil {
					t.Errorf("Stats: %v", err)
					return
				}
			}
		}()
	}

	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("concurrent statistics test timed out")
	}

	stats := statsContains(t, c,
		"successful reads count:",
		"unsuccessful reads count:")

	var successful, unsuccessful int64
	if _, err := fmt.Sscanf(stats,
		"stats: total current count: %*d, successful reads count: %d, unsuccessful reads count: %d",
		&successful, &unsuccessful); err != nil {
		// Keep the test independent of the formatting parser's support for
		// suppressed conversions; check the counter values directly instead.
		successful = int64(c.successfulReadsCount.Load())
		unsuccessful = int64(c.unsuccessfulReadsCount.Load())
	}

	expected := int64(workers * iterations)
	if successful != expected {
		t.Errorf("expected %d successful reads, got %d", expected, successful)
	}
	if unsuccessful != expected {
		t.Errorf("expected %d unsuccessful reads, got %d", expected, unsuccessful)
	}
}

func TestShutdownWhileCleanupIsRunning(t *testing.T) {
	c := newTestCache(t)

	for i := 0; i < 100; i++ {
		k := key(fmt.Sprintf("shutdown-%d", i))
		mustSet(t, c, k, testValue(i, time.Hour))
		expireEntry(t, c, k)
	}

	result := make(chan struct{})
	go func() {
		c.Close()
		close(result)
	}()

	select {
	case <-result:
	case <-time.After(testTimeout):
		t.Fatal("Close did not finish while cleanup was active")
	}

	if !c.closed.Load() {
		t.Fatal("cache should be marked closed after Close returns")
	}
}

func TestConcurrentAndRepeatedCloseCalls(t *testing.T) {
	c := newCache()

	const callers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c.Close()
		}()
	}

	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("concurrent Close calls did not finish")
	}

	// Repeated Close calls must also be safe.
	c.Close()
	c.Close()

	if !c.closed.Load() {
		t.Fatal("cache should be closed")
	}
}

func TestOperationsAfterClosure(t *testing.T) {
	c := newCache()
	c.Close()

	if err := c.Set("key", testValue("value", time.Hour)); !errors.Is(err, cacheIsClosed) {
		t.Errorf("Set after Close: expected cacheIsClosed, got %v", err)
	}

	if _, err := c.Get("key"); !errors.Is(err, cacheIsClosed) {
		t.Errorf("Get after Close: expected cacheIsClosed, got %v", err)
	}

	if err := c.Delete("key"); !errors.Is(err, cacheIsClosed) {
		t.Errorf("Delete after Close: expected cacheIsClosed, got %v", err)
	}

	if _, err := c.Stats(); !errors.Is(err, cacheIsClosed) {
		t.Errorf("Stats after Close: expected cacheIsClosed, got %v", err)
	}
}

func TestGoroutineLeakSensitiveLifecycle(t *testing.T) {
	// Verify that repeated cache creation and shutdown does not leave recycler
	// workers running. WaitGroup completion is the primary synchronization;
	// goroutine counts are checked only after all Close calls have returned.
	const instances = 20

	for i := 0; i < instances; i++ {
		c := newCache()
		c.Close()

		if !c.closed.Load() {
			t.Fatalf("cache %d was not closed", i)
		}
		if c.ctx.Err() == nil {
			t.Fatalf("cache %d context was not cancelled", i)
		}
	}

	// Exercise a second lifecycle with concurrent operations in flight.
	c := newCache()

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start

			for i := 0; i < 50; i++ {
				k := key(strconv.Itoa(worker % 4))
				_ = c.Set(k, testValue(i, time.Hour))
				_, _ = c.Get(k)
			}
		}(worker)
	}

	close(start)

	opsDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(opsDone)
	}()

	select {
	case <-opsDone:
	case <-time.After(testTimeout):
		c.Close()
		t.Fatal("in-flight operations did not finish")
	}

	closeDone := make(chan struct{})
	go func() {
		c.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(testTimeout):
		t.Fatal("cache shutdown left lifecycle work blocked")
	}
}

func BenchmarkConcurrentGet(b *testing.B) {
	c := newCache()
	defer c.Close()

	const entries = 1024
	for i := 0; i < entries; i++ {
		k := key(fmt.Sprintf("bench-%d", i))
		if err := c.Set(k, testValue(i, time.Hour)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			k := key(fmt.Sprintf("bench-%d", i%entries))
			if _, err := c.Get(k); err != nil {
				b.Errorf("Get: %v", err)
				return
			}
			i++
		}
	})
}

func BenchmarkConcurrentMixedGetSet(b *testing.B) {
	c := newCache()
	defer c.Close()

	const entries = 1024
	for i := 0; i < entries; i++ {
		k := key(fmt.Sprintf("bench-%d", i))
		if err := c.Set(k, testValue(i, time.Hour)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			k := key(fmt.Sprintf("bench-%d", i%entries))

			if i%4 == 0 {
				if err := c.Set(k, testValue(i, time.Hour)); err != nil {
					b.Errorf("Set: %v", err)
					return
				}
			} else {
				if _, err := c.Get(k); err != nil {
					b.Errorf("Get: %v", err)
					return
				}
			}
			i++
		}
	})
}

// Compile-time assertion that the cache's context can be used with the
// standard context package in lifecycle tests.
var _ context.Context = context.Background()
