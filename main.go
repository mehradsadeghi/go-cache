package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	shardsCount       = 16
	recyclingInterval = time.Second
	maxTtlDuration    = time.Hour * 365 * 24 // 1 year
)

var (
	keyHasExpired = errors.New("key has expired")
	keyNotFound   = errors.New("key not found")
	cacheIsClosed = errors.New("cache is closed")
)

type key string

type value struct {
	createdAt time.Time
	TTL       time.Duration
	Data      any
}

type keyValuePairs struct {
	mu    *sync.RWMutex
	items map[key]value
}

type cache struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	closed                 atomic.Bool
	ClosedOnce             sync.Once
	cache                  map[uint32]keyValuePairs
	keys                   []key
	currentEntryCount      atomic.Int32
	successfulReadsCount   atomic.Int32
	unsuccessfulReadsCount atomic.Int32
}

func newCache() *cache {
	c := make(map[uint32]keyValuePairs)
	for i := range shardsCount {
		c[uint32(i)] = keyValuePairs{mu: &sync.RWMutex{}, items: make(map[key]value)}
	}

	ctx, cancel := context.WithCancel(context.Background())

	cc := &cache{
		ctx:    ctx,
		cancel: cancel,
		wg:     sync.WaitGroup{},
		keys:   make([]key, 0),
		cache:  c,
	}

	cc.startRecycling()

	return cc
}

func (c *cache) startRecycling() {
	if c.closed.Load() {
		return
	}

	for shardNumber := range shardsCount {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-time.After(recyclingInterval):
					c.cache[uint32(shardNumber)].mu.Lock()
					items := c.cache[uint32(shardNumber)].items
					c.cache[uint32(shardNumber)].mu.Unlock()

					for k, _ := range items {
						v, err := c.get(k)
						if (err != nil && errors.Is(err, keyHasExpired)) || expired(v) {
							_ = c.delete(k)
						}
					}

				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
}

func (c *cache) set(k key, v value) {
	if c.closed.Load() {
		return
	}

	select {
	case <-c.ctx.Done():
		return
	default:
		if !isValidDuration(v.TTL) {
			return
		}

		shard := getShardNumber(k)
		c.cache[shard].mu.Lock()
		defer c.cache[shard].mu.Unlock()

		_, ok := c.cache[shard].items[k]
		if !ok {
			c.currentEntryCount.Add(1)
		}

		c.cache[getShardNumber(k)].items[k] = value{
			createdAt: time.Now(),
			TTL:       v.TTL,
			Data:      v.Data,
		}
	}
}

func (c *cache) get(k key) (value, error) {
	if c.closed.Load() {
		return value{}, cacheIsClosed
	}

	select {
	case <-c.ctx.Done():
		return value{}, c.ctx.Err()
	default:
		shard := getShardNumber(k)
		c.cache[shard].mu.Lock()
		defer c.cache[shard].mu.Unlock()

		v, ok := c.cache[shard].items[k]
		if !ok {
			c.unsuccessfulReadsCount.Add(1)
			return value{}, keyNotFound
		}
		if expired(v) {
			c.unsuccessfulReadsCount.Add(1)
			return value{}, keyHasExpired
		}

		c.successfulReadsCount.Add(1)
		return v, nil
	}
}

func (c *cache) delete(k key) error {
	if c.closed.Load() {
		return cacheIsClosed
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
		shard := getShardNumber(k)
		c.cache[shard].mu.Lock()
		defer c.cache[shard].mu.Unlock()

		_, ok := c.cache[shard].items[k]
		if !ok {
			return keyNotFound
		}

		delete(c.cache[shard].items, k)

		return nil
	}
}

func (c *cache) stats() string {
	if c.closed.Load() {
		return cacheIsClosed.Error()
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err().Error()
	default:
		return fmt.Sprintf("stats: total current count: %d, successful reads count: %d, unsuccessful reads count: %d", c.currentEntryCount.Load(), c.successfulReadsCount.Load(), c.unsuccessfulReadsCount.Load())
	}
}

func (c *cache) close() {
	c.ClosedOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		c.closed.Store(true)
	})
}

func main() {
	c := newCache()
	c.set("A", value{TTL: time.Second * 3, Data: "a cached page A"})
	<-time.After(time.Second * 2)
	v, err := c.get("A")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(v.Data)
	}
}

func getShardNumber(key key) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	// Map the hash to the range [0, 15].
	return uint32(h.Sum32() % shardsCount)
}

func expired(v value) bool {
	return time.Since(v.createdAt) >= v.TTL
}

func isValidDuration(d time.Duration) bool {
	return d > 0 && d <= maxTtlDuration
}
