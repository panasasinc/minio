package cmd

import (
	"errors"
	"sync"
)

var errCacheEntryNotFound = errors.New("Entry not found in cache")

type BucketMetadataCache struct {
	lock     sync.Mutex
	metadata *map[string]BucketMetadata
}

// NewBucketMetadataCache returns a pointer to a newly created BucketMetadataCache object
func NewBucketMetadataCache() *BucketMetadataCache {
	return &BucketMetadataCache{
		metadata: new(map[string]BucketMetadata),
	}
}

// Get fetches the given value from the cache
func (cache *BucketMetadataCache) Get(bucket string) (BucketMetadata, error) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	meta, ok := (*cache.metadata)[bucket]
	if ok {
		return meta, nil
	}

	return BucketMetadata{}, errCacheEntryNotFound
}

// Set sets the value for the given entry in the cache
func (cache *BucketMetadataCache) Set(bucket string, metadata BucketMetadata) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	(*cache.metadata)[bucket] = metadata
}

// Delete removes a given entry from the cache
func (cache *BucketMetadataCache) Delete(bucket string) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	delete(*cache.metadata, bucket)
}

// Drop removes all the entries from the cache
func (cache *BucketMetadataCache) Drop() {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.metadata = new(map[string]BucketMetadata)
}
