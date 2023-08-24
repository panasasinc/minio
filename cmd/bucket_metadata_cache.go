// Copyright (c) 2022-2023 Panasas, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"errors"
	"sync"
)

var errCacheEntryNotFound = errors.New("Entry not found in cache")

type BucketMetadataCache struct {
	mutex    sync.RWMutex
	metadata map[string]BucketMetadata
}

// NewBucketMetadataCache returns a pointer to a newly created BucketMetadataCache object
func NewBucketMetadataCache() *BucketMetadataCache {
	return &BucketMetadataCache{
		metadata: make(map[string]BucketMetadata),
	}
}

// rlock locks cache for reading
func (cache *BucketMetadataCache) rlock() {
	cache.mutex.RLock()
}

// runlock unlocks cache locked for reading
func (cache *BucketMetadataCache) runlock() {
	cache.mutex.RUnlock()
}

// lock locks cache for writing
func (cache *BucketMetadataCache) lock() {
	cache.mutex.Lock()
}

// unlock unlocks cache locked for writing
func (cache *BucketMetadataCache) unlock() {
	cache.mutex.Unlock()
}

// Get fetches the given value from the cache
func (cache *BucketMetadataCache) Get(bucket string) (BucketMetadata, error) {
	if cache == nil {
		return BucketMetadata{}, errCacheEntryNotFound
	}
	cache.rlock()
	defer cache.runlock()

	meta, ok := cache.metadata[bucket]
	if ok {
		return meta, nil
	}

	return BucketMetadata{}, errCacheEntryNotFound
}

// Set sets the value for the given entry in the cache
func (cache *BucketMetadataCache) Set(bucket string, metadata BucketMetadata) {
	if cache == nil {
		return
	}
	cache.lock()
	defer cache.unlock()
	cache.metadata[bucket] = metadata
}

// Delete removes a given entry from the cache
func (cache *BucketMetadataCache) Delete(bucket string) {
	if cache == nil {
		return
	}
	cache.lock()
	defer cache.unlock()
	delete(cache.metadata, bucket)
}

// Drop removes all the entries from the cache
func (cache *BucketMetadataCache) Drop() {
	if cache == nil {
		return
	}
	cache.lock()
	defer cache.unlock()
	cache.metadata = make(map[string]BucketMetadata)
}
