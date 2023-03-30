// TODO: Add license header
package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio/internal/util/cache"
)

var errNotFoundInCache = errors.New("Item not found in cache")

// CachingConfigAccessor can be used with the config operations implemented in
// config-common.go (readConfig(), deleteConfig(), saveConfig()) to cache the
// most frequently accessed data.
type CachingConfigAccessor struct {
	cache       *cache.Cache
	rawAccessor configAccessor
}

func newCachingConfigAccessor(raw configAccessor) *CachingConfigAccessor {
	cacheParams := cache.CacheParams{
		MaxL1Count:         64,
		MaxL2Count:         512,
		MaxBytes:           1048576,
		CompactionInterval: 10 * time.Minute,
	}
	newAccessor := CachingConfigAccessor{
		rawAccessor: raw,
		cache:       cache.New(cacheParams),
	}
	return &newAccessor
}

func (cca *CachingConfigAccessor) getCachedObjectNInfo(ctx context.Context, objectPath string) (reader *GetObjectReader, err error) {
	data := cca.cache.Get(ctx, objectPath)
	if data == nil {
		return nil, errNotFoundInCache
	}
	r := bytes.NewReader(data)
	return NewGetObjectReaderFromReader(r, ObjectInfo{}, ObjectOptions{})
}

func (cca *CachingConfigAccessor) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error) {
	// While normally GetObjectNInfo() returns a lot of data (ObjInfo,
	// cleanUpFns, opts, once), BucketMetadata operations use readConfig
	// which discards all but the actual object content.

	key := pathJoin(bucket, object)
	r, err := cca.getCachedObjectNInfo(ctx, key)
	if err == nil {
		return r, err
	}

	if !errors.Is(err, errNotFoundInCache) {
		return nil, err
	}

	// Item not found in cache. Use the rawAccessor and put the
	// item in cache.
	rawReader, err := cca.rawAccessor.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
	if err != nil {
		return nil, err
	}
	defer rawReader.Close()
	data, err := io.ReadAll(rawReader)
	cca.cache.Put(ctx, key, data)

	return NewGetObjectReaderFromReader(bytes.NewReader(data), ObjectInfo{}, ObjectOptions{})
}

func (cca *CachingConfigAccessor) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Instead of unwrapping the data and re-wrapping it for storage, let's
	// just invalidate the cache entry.
	key := pathJoin(bucket, object)
	cca.cache.Invalidate(ctx, key)

	return cca.rawAccessor.PutObject(ctx, bucket, object, data, opts)
}

func (cca *CachingConfigAccessor) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	key := pathJoin(bucket, object)
	cca.cache.Delete(ctx, key)

	return cca.rawAccessor.DeleteObject(ctx, bucket, object, opts)
}
