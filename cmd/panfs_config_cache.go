// TODO: Add license header
package cmd

import (
	"context"
	"net/http"
	"time"

	"github.com/minio/minio/internal/util/cache"
)

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

func (cca *CachingConfigAccessor) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error) {
	return cca.rawAccessor.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}

func (cca *CachingConfigAccessor) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return cca.rawAccessor.PutObject(ctx, bucket, object, data, opts)
}

func (cca *CachingConfigAccessor) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return cca.rawAccessor.DeleteObject(ctx, bucket, object, opts)
}
