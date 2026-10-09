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
)

var (
	keyHasExpired = errors.New("key has expired")
	keyNotFound   = errors.New("key not found")
)

type key string

type value struct {
	createdAt time.Time
	TTL       time.Duration
	Data      string
}

type keyValuePairs struct {
	mu    *sync.RWMutex
	items map[key]value
}

type cache struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	mu                     sync.Mutex
	cache                  map[uint32]keyValuePairs
	keys                   []key
	currentEntryCount      atomic.Int32
	successfulReadsCount   atomic.Int32
	unsuccessfulReadsCount atomic.Int32
}

func NewCache() *cache {
	c := make(map[uint32]keyValuePairs)
	for i := range shardsCount {
		c[uint32(i)] = keyValuePairs{mu: &sync.RWMutex{}, items: make(map[key]value)}
	}

	ctx, cancel := context.WithCancel(context.Background())

	cc := &cache{
		ctx:    ctx,
		cancel: cancel,
		mu:     sync.Mutex{},
		keys:   make([]key, 0),
		cache:  c,
	}

	cc.startRecycling()

	return cc
}

func (c *cache) startRecycling() {
	go func() {
		for {
			select {
			case <-time.After(recyclingInterval):
				c.mu.Lock()
				ks := c.keys
				c.mu.Unlock()
				for _, k := range ks {
					v, err := c.get(k)
					if err != nil {
						if errors.Is(err, keyHasExpired) {
							_ = c.delete(k)
						}
						continue
					}
					if expired(v) {
						_ = c.delete(k)
					}
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

func expired(v value) bool {
	return time.Since(v.createdAt) >= v.TTL
}

func (c *cache) set(k key, v value) {
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

	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = append(c.keys, k)
}

func (c *cache) get(k key) (value, error) {
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

func (c *cache) delete(k key) error {
	shard := getShardNumber(k)
	c.cache[shard].mu.Lock()
	defer c.cache[shard].mu.Unlock()

	_, ok := c.cache[shard].items[k]
	if !ok {
		return errors.New("key " + string(k) + " not found")
	}

	delete(c.cache[shard].items, k)

	return nil
}

func (c *cache) stats() string {
	return fmt.Sprintf("stats: total current count: %d, successful reads count: %d, unsuccessful reads count: %d", c.currentEntryCount.Load(), c.successfulReadsCount.Load(), c.unsuccessfulReadsCount.Load())
}

func main() {
	c := NewCache()
	c.set("A", value{TTL: time.Second * 2, Data: "a cached page A"})
	c.set("B", value{TTL: time.Second * 2, Data: "a cached page B"})
	<-time.After(time.Second)
	v, err := c.get("A")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(v.Data)
	}
	/*err = c.delete("A")
	if err != nil {
		fmt.Println(err)
	}*/

	//c.Close()
}

func getShardNumber(key key) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	// Map the hash to the range [1, 16].
	return uint32(h.Sum32() % shardsCount)
}
