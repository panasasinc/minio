// Copyright (c) 2015-2021 MinIO, Inc.
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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/minio/minio/internal/hash"
)

type configReader interface {
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error)
}

type configSaver interface {
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
}

type configDeleter interface {
	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
}

type configAccessor interface {
	configReader
	configSaver
	configDeleter
}

var errConfigNotFound = errors.New("config file not found")

func readConfigWithMetadata(ctx context.Context, store configReader, configFile string) ([]byte, ObjectInfo, error) {
	r, err := store.GetObjectNInfo(ctx, minioMetaBucket, configFile, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		// Treat object not found as config not found.
		if isErrObjectNotFound(err) {
			return nil, ObjectInfo{}, errConfigNotFound
		}

		return nil, ObjectInfo{}, err
	}
	defer r.Close()

	buf, err := io.ReadAll(r)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if len(buf) == 0 {
		return nil, ObjectInfo{}, errConfigNotFound
	}
	return buf, r.ObjInfo, nil
}

func readConfig(ctx context.Context, store configReader, configFile string) ([]byte, error) {
	buf, _, err := readConfigWithMetadata(ctx, store, configFile)
	return buf, err
}

func deleteConfig(ctx context.Context, deleter configDeleter, configFile string) error {
	_, err := deleter.DeleteObject(ctx, minioMetaBucket, configFile, ObjectOptions{
		DeletePrefix: true,
	})
	if err != nil && isErrObjectNotFound(err) {
		return errConfigNotFound
	}
	return err
}

func saveConfig(ctx context.Context, store configSaver, configFile string, data []byte) error {
	hashReader, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", getSHA256Hash(data), int64(len(data)))
	if err != nil {
		return err
	}

	_, err = store.PutObject(ctx, minioMetaBucket, configFile, NewPutObjReader(hashReader), ObjectOptions{MaxParity: true})
	return err
}

func checkConfig(ctx context.Context, objAPI ObjectLayer, configFile string) error {
	if _, err := objAPI.GetObjectInfo(ctx, minioMetaBucket, configFile, ObjectOptions{}); err != nil {
		// Treat object not found as config not found.
		if isErrObjectNotFound(err) {
			return errConfigNotFound
		}

		return err
	}
	return nil
}
