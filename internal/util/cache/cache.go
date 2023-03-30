// TODO: Add license header
// Package cache implements two-level cache where:
//   - L1 cache uses LFU (least-frequently-used) cache replacement policy
//     implemented using a heap,
//   - L2 cache uses a list-based LRU (least-recently-used) cache replacement
//     policy.
//
// L1 stores data, L2 only stores metadata for L1.
package cache

import (
	"container/heap"
	"container/list"
	"context"
	"sync"
	"time"
)

type l1CacheEntry struct {
	id       string
	content  []byte
	rank     int
	hits     int
	lfuIndex int
}

type l1LFU []*l1CacheEntry

func (lfu l1LFU) Len() int { return len(lfu) }

// The minimum element is the one with fewer hits.
// If two elements have equal number of hits, the newer one is the lesser.
func (lfu l1LFU) Less(i, j int) bool {
	if lfu[i].hits == lfu[j].hits {
		// We want Pop to give us the newest element so we use greater
		// than here:
		return lfu[i].rank > lfu[j].rank
	}
	return lfu[i].hits < lfu[j].hits
}

func (lfu l1LFU) Swap(i, j int) {
	lfu[i], lfu[j] = lfu[j], lfu[i]
	lfu[i].lfuIndex = i
	lfu[j].lfuIndex = j
}

func (lfu *l1LFU) Push(x any) {
	n := len(*lfu)
	entry := x.(*l1CacheEntry)
	entry.lfuIndex = n
	*lfu = append(*lfu, entry)
}

func (lfu *l1LFU) Pop() any {
	old := *lfu
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil      // avoid memory leak
	entry.lfuIndex = -1 // for safety
	*lfu = old[0 : n-1]
	return entry
}

type l1Cache struct {
	index map[string]*l1CacheEntry
	lfu   l1LFU // heap ordered by hit counters + ranks
}

type l2CacheEntry struct {
	id         string
	rank       int
	hits       int
	lruElement *list.Element
}

type l2Cache struct {
	index map[string]*l2CacheEntry
	lru   list.List // elements in the list are ordered from oldest to most recent
}

type CacheParams struct {
	MaxL1Count         int           `json:"max_l1_count"`
	MaxL2Count         int           `json:"max_l2_count"`
	MaxBytes           int           `json:"max_bytes"`
	CompactionInterval time.Duration `json:"compaction_interval"`
}

type Cache struct {
	params     CacheParams
	rank       int
	totalBytes int
	lock       *sync.Mutex
	l1         l1Cache // LFU - heap-based
	l2         l2Cache // LRU - list-based
}

func New(params CacheParams) *Cache {
	return &Cache{
		params:     params,
		rank:       0,
		totalBytes: 0,
		lock:       &sync.Mutex{},
		l1: l1Cache{
			index: map[string]*l1CacheEntry{},
			lfu:   make([]*l1CacheEntry, 0, params.MaxL1Count),
		},
		l2: l2Cache{
			index: map[string]*l2CacheEntry{},
			lru:   list.List{},
		},
	}
}

func (c *Cache) Get(ctx context.Context, id string) []byte {
	c.lock.Lock()
	defer c.lock.Unlock()

	// search only L1 cache for values, L2 only contains some metadata
	entry, ok := c.l1.index[id]
	if !ok {
		return nil
	}

	entry.hits++
	heap.Fix(&c.l1.lfu, entry.lfuIndex)

	return entry.content
}

func (c *Cache) Put(ctx context.Context, id string, content []byte) {
	if len(content) > int(c.params.MaxBytes) {
		// TODO: log value is too big
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	l1Entry, ok := c.l1.index[id]
	if ok {
		l1Entry.content = content
		l1Entry.hits++
		heap.Fix(&c.l1.lfu, l1Entry.lfuIndex)
		return
	}

	l1Entry = &l1CacheEntry{
		id:      id,
		content: content,
	}

	l2Entry, ok := c.l2.index[id]
	if ok {
		l1Entry.hits = l2Entry.hits
		l1Entry.rank = l2Entry.rank

		delete(c.l2.index, id)
		c.l2.lru.Remove(l2Entry.lruElement)
	} else {
		c.rank++
		l1Entry.rank = c.rank
		l1Entry.hits = 0
	}

	for !(len(c.l1.index) < c.params.MaxL1Count && c.totalBytes+len(content) <= c.params.MaxBytes) {
		c.l1EvictLocked(ctx)
	}

	c.l1.index[id] = l1Entry
	heap.Push(&c.l1.lfu, l1Entry)

	c.totalBytes += len(content)
}

// Delete removes the item with the given id from the cache entirely
func (c *Cache) Delete(ctx context.Context, id string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	entry, ok := c.l1.index[id]
	if !ok {
		return
	}

	heap.Remove(&c.l1.lfu, entry.lfuIndex)
	delete(c.l1.index, entry.id)
}

// Delete moves the item with the given id from the LFU to the LRU cache.
// This is equivalent to invalidating the item data but not removing the item's
// metadata.
func (c *Cache) Invalidate(ctx context.Context, id string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	l1Entry, ok := c.l1.index[id]
	if !ok {
		return
	}
	c.l1RemoveEntry(ctx, l1Entry.lfuIndex)
}

func (c *Cache) l1EvictLocked(ctx context.Context) {
	c.l1RemoveEntry(ctx, 0)
}

func (c *Cache) l1RemoveEntry(ctx context.Context, entryIdx int) {
	l1Entry := heap.Remove(&c.l1.lfu, entryIdx).(*l1CacheEntry)
	if l1Entry == nil {
		return
	}
	delete(c.l1.index, l1Entry.id)

	for len(c.l2.index) >= c.params.MaxL2Count {
		c.l2EvictLocked(ctx)
	}

	c.totalBytes -= len(l1Entry.content)
	l1Entry.content = nil

	l2Entry := &l2CacheEntry{
		id:         l1Entry.id,
		rank:       l1Entry.rank,
		hits:       l1Entry.hits,
		lruElement: nil,
	}

	c.l2.index[l2Entry.id] = l2Entry
	l2Entry.lruElement = c.l2.lru.PushBack(l2Entry)
}

func (c *Cache) l2EvictLocked(ctx context.Context) {
	firstElem := c.l2.lru.Front()
	if firstElem == nil {
		return
	}
	l2Entry := c.l2.lru.Remove(firstElem).(*l2CacheEntry)
	l2Entry.lruElement = nil
	delete(c.l2.index, l2Entry.id)
}

func (c *Cache) L1Count() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.l1.index)
}

func (c *Cache) L2Count() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.l2.index)
}

func (c *Cache) TotalBytes() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.totalBytes
}
