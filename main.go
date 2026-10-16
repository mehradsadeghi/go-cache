package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
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
	KeyHasExpired = errors.New("key has expired")
	KeyNotFound   = errors.New("key not found")
	CacheIsClosed = errors.New("cache is closed")
	InvalidTtl    = errors.New("invalid TTL")
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

type Cache struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	mu                     sync.Mutex
	closed                 atomic.Bool
	closedOnce             sync.Once
	cache                  map[uint32]keyValuePairs
	keys                   []key
	currentEntryCount      atomic.Int32
	successfulReadsCount   atomic.Int32
	unsuccessfulReadsCount atomic.Int32
}

func NewCache() *Cache {
	c := make(map[uint32]keyValuePairs)
	for i := range shardsCount {
		c[uint32(i)] = keyValuePairs{mu: &sync.RWMutex{}, items: make(map[key]value)}
	}

	ctx, cancel := context.WithCancel(context.Background())

	cc := &Cache{
		ctx:    ctx,
		cancel: cancel,
		wg:     sync.WaitGroup{},
		mu:     sync.Mutex{},
		keys:   make([]key, 0),
		cache:  c,
	}

	cc.startRecycling()

	return cc
}

func (c *Cache) startRecycling() {
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
					items := make(map[key]value)
					c.cache[uint32(shardNumber)].mu.Lock()
					for k, v := range c.cache[uint32(shardNumber)].items {
						items[k] = v
					}
					c.cache[uint32(shardNumber)].mu.Unlock()

					for k, _ := range items {
						v, err := c.Get(k)
						if (err != nil && errors.Is(err, KeyHasExpired)) || expired(v) {
							_ = c.Delete(k)
						}
					}

				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
}

func (c *Cache) Set(k key, v value) error {
	if c.closed.Load() {
		return CacheIsClosed
	}
	c.mu.Lock()
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
		if !isValidDuration(v.TTL) {
			return InvalidTtl
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

	return nil
}

func (c *Cache) Get(k key) (value, error) {
	if c.closed.Load() {
		return value{}, CacheIsClosed
	}
	c.mu.Lock()
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()

	select {
	case <-c.ctx.Done():
		return value{}, c.ctx.Err()
	default:
		shard := getShardNumber(k)
		c.cache[shard].mu.RLock()
		defer c.cache[shard].mu.RUnlock()

		v, ok := c.cache[shard].items[k]
		if !ok {
			c.unsuccessfulReadsCount.Add(1)
			return value{}, KeyNotFound
		}
		if expired(v) {
			c.unsuccessfulReadsCount.Add(1)
			return value{}, KeyHasExpired
		}

		c.successfulReadsCount.Add(1)
		return v, nil
	}
}

func (c *Cache) Delete(k key) error {
	if c.closed.Load() {
		return CacheIsClosed
	}
	c.mu.Lock()
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
		shard := getShardNumber(k)
		c.cache[shard].mu.Lock()
		defer c.cache[shard].mu.Unlock()

		_, ok := c.cache[shard].items[k]
		if !ok {
			return KeyNotFound
		}

		delete(c.cache[shard].items, k)

		c.currentEntryCount.Add(-1)

		return nil
	}
}

func (c *Cache) Stats() (string, error) {
	if c.closed.Load() {
		return "", CacheIsClosed
	}
	c.mu.Lock()
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()

	select {
	case <-c.ctx.Done():
		return "", c.ctx.Err()
	default:
		return fmt.Sprintf("stats: total current count: %d, successful reads count: %d, unsuccessful reads count: %d", c.currentEntryCount.Load(), c.successfulReadsCount.Load(), c.unsuccessfulReadsCount.Load()), nil
	}
}

func (c *Cache) Close() {
	c.closedOnce.Do(func() {
		c.mu.Lock()
		c.cancel()
		c.mu.Unlock()
		c.wg.Wait()
		c.closed.Store(true)
	})
}

func getShardNumber(key key) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	// Map the hash to the range [0, 15].
	return h.Sum32() % shardsCount
}

func expired(v value) bool {
	return time.Since(v.createdAt) >= v.TTL
}

func isValidDuration(d time.Duration) bool {
	return d > 0 && d <= maxTtlDuration
}

func main() {
	c := NewCache()

	for i := range 100 {
		go func() {
			_ = c.Set(key(strconv.Itoa(i)), value{TTL: time.Second * 3, Data: "a cached page " + strconv.Itoa(i)})
		}()
	}

	for i := range 100 {
		v, err := c.Get(key(strconv.Itoa(i)))
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(v)
		}
	}

	for i := range 100 {
		go func() { _ = c.Delete(key(strconv.Itoa(i))) }()
	}

	for range 100 {
		go c.Close()
	}

	for range 100 {
		go func() { _, _ = c.Stats() }()
	}

	<-time.After(time.Second * 2)
}
