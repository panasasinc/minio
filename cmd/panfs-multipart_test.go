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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/internal/config/api"
)

// initPanFSWithBucket initializes the panfs backend and creates a bucket for testing
// Fail test when object init or bucket creation will fail
func initPanFSWithBucket(bucket string, t *testing.T) (obj ObjectLayer, disk string) {
	disk = filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())

	obj, err := initPanFSObjects(disk)
	obj = obj.(*PANFSObjects)
	if err != nil {
		t.Fatalf("Cannot init PANFS backend: \"%v\"", err)
	}
	if bucket != "" {
		err = obj.MakeBucketWithLocation(GlobalContext, bucket, MakeBucketOptions{PanFSBucketPath: disk})
		if err != nil {
			t.Fatalf("Cannot create bucket \"%v\"", err)
		}
	}
	return
}

// Tests cleanup multipart uploads for filesystem backend.
func TestPANFSCleanupMultipartUploadsInRoutine(t *testing.T) {
	t.Skip()
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*PANFSObjects)

	bucketName := "bucket"
	objectName := "object"

	// Create a context we can cancel.
	ctx, cancel := context.WithCancel(GlobalContext)
	obj.MakeBucketWithLocation(ctx, bucketName, MakeBucketOptions{})

	res, err := obj.NewMultipartUpload(ctx, bucketName, objectName, ObjectOptions{})
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	globalAPIConfig.init(api.Config{
		ListQuorum:                  "optimal",
		StaleUploadsExpiry:          time.Millisecond,
		StaleUploadsCleanupInterval: time.Millisecond,
	}, obj.SetDriveCounts())

	defer func() {
		globalAPIConfig.init(api.Config{
			ListQuorum: "optimal",
		}, obj.SetDriveCounts())
	}()

	var cleanupWg sync.WaitGroup
	cleanupWg.Add(1)
	go func() {
		defer cleanupWg.Done()
		fs.cleanupStaleUploads(ctx)
	}()

	// Wait for 100ms such that - we have given enough time for
	// cleanup routine to kick in. Flaky on slow systems...
	time.Sleep(100 * time.Millisecond)
	cancel()
	cleanupWg.Wait()

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(GlobalContext, bucketName, objectName, res.UploadID, ObjectOptions{}); err != nil {
		if _, ok := err.(InvalidUploadID); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	} else {
		t.Error("Item was not cleaned up.")
	}
}

// TestNewPANFMultipartUploadFaultyDisk - test NewMultipartUpload with faulty disks
func TestNewPANFMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)

	// Test with disk removed.
	os.RemoveAll(disk)
	if _, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestPANFSPutObjectPartFaultyDisk - test PutObjectPart with faulty disks
func TestPANFSPutObjectPartFaultyDisk(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)
	data := []byte("12345")
	dataLen := int64(len(data))

	res, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)
	sha256sum := ""

	obj, newDisk := initPanFSWithBucket("", t)
	defer os.RemoveAll(newDisk)
	if _, err = obj.PutObjectPart(GlobalContext, bucketName, objectName, res.UploadID, 1, mustGetPutObjReader(t, bytes.NewReader(data), dataLen, md5Hex, sha256sum), ObjectOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestPANFSCompleteMultipartUploadFaultyDisk - test CompleteMultipartUpload with faulty disks
func TestPANFSCompleteMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)
	data := []byte("12345")

	res, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)

	parts := []CompletePart{{PartNumber: 1, ETag: md5Hex}}
	obj, newDisk := initPanFSWithBucket("", t)
	defer os.RemoveAll(newDisk)
	if _, err := obj.CompleteMultipartUpload(GlobalContext, bucketName, objectName, res.UploadID, parts, ObjectOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestPANFSCompleteMultipartUpload - test CompleteMultipartUpload
func TestPANFSCompleteMultipartUpload(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)

	defer os.RemoveAll(disk)
	data := []byte("12345")

	res, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	checkMultipartTempFolder(t, obj, res, 1, 0)

	md5Hex := getMD5Hash(data)

	if _, err := obj.PutObjectPart(GlobalContext, bucketName, objectName, res.UploadID, 1, mustGetPutObjReader(t, bytes.NewReader(data), 5, md5Hex, ""), ObjectOptions{}); err != nil {
		t.Fatal("Unexpected error ", err)
	}
	checkMultipartTempFolder(t, obj, res, 1, 0)

	parts := []CompletePart{{PartNumber: 1, ETag: md5Hex}}
	if _, err := obj.CompleteMultipartUpload(GlobalContext, bucketName, objectName, res.UploadID, parts, ObjectOptions{}); err != nil {
		t.Fatal("Unexpected error ", err)
	}
	checkMultipartTempFolder(t, obj, nil, 0, 0)
}

// TestPANFSAbortMultipartUpload - test CompleteMultipartUpload
func TestPANFSAbortMultipartUpload(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		// Concurrent AbortMultipartUpload() fails on windows
		t.Skip()
	}

	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)
	data := []byte("12345")

	res, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	checkMultipartTempFolder(t, obj, res, 1, 0)

	md5Hex := getMD5Hash(data)

	opts := ObjectOptions{}
	if _, err := obj.PutObjectPart(GlobalContext, bucketName, objectName, res.UploadID, 1, mustGetPutObjReader(t, bytes.NewReader(data), 5, md5Hex, ""), opts); err != nil {
		t.Fatal("Unexpected error ", err)
	}
	if err := obj.AbortMultipartUpload(GlobalContext, bucketName, objectName, res.UploadID, opts); err != nil {
		t.Fatal("Unexpected error ", err)
	}
	checkMultipartTempFolder(t, obj, nil, 0, 0)
}

// TestPANFSListMultipartUploadsFaultyDisk - test ListMultipartUploads with faulty disks
func TestPANFSListMultipartUploadsFaultyDisk(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)

	_, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	obj, disk = initPanFSWithBucket("", t)
	defer os.RemoveAll(disk)
	if _, err := obj.ListMultipartUploads(GlobalContext, bucketName, objectName, "", "", "", 1000); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

func TestPANFSNewMultipartUpload(t *testing.T) {
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)

	// Create new multipart upload using not existing bucket
	nonExistentBucket := "non-existent-bucket"
	_, err := obj.NewMultipartUpload(GlobalContext, nonExistentBucket, "test-object", ObjectOptions{})
	if !errors.Is(err, BucketNotFound{Bucket: nonExistentBucket}) {
		t.Fatalf("Expected error \"%v\" but found \"%v\"", BucketNotFound{Bucket: nonExistentBucket}, err)
	}

	// Create new multipart upload using valid bucket and object
	_, err = obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{})
	if err != nil {
		t.Fatalf("Unexpected error \"%v\"", err)
	}
}

func TestPANFSPutObjectPart(t *testing.T) {
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	contentBytes := []byte("content")
	sha256 := getSHA256Hash(contentBytes)
	partNumber := 1
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)

	nonExistendUploadID := "nonExistentUploadID"
	nonExistentBucket := "nonExistentBucket"

	uploadID, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{})
	if err != nil {
		t.Fatalf("Unexpected error \"%v\"", err)
	}

	reader := mustGetPutObjReader(t, bytes.NewReader(contentBytes), int64(len(contentBytes)), "", sha256)

	// Put object part with not existing bucket
	_, err = obj.PutObjectPart(GlobalContext, nonExistentBucket, objectName, uploadID.UploadID, partNumber, reader, ObjectOptions{})
	if !errors.Is(err, BucketNotFound{Bucket: nonExistentBucket}) {
		t.Fatalf("Expected error \"%v\" but found \"%v\"", BucketNotFound{Bucket: nonExistentBucket}, err)
	}

	// Put object part with an uninitialized upload id
	_, err = obj.PutObjectPart(GlobalContext, bucketName, objectName, nonExistendUploadID, partNumber, reader, ObjectOptions{})
	expectedErr := InvalidUploadID{Bucket: bucketName, Object: objectName, UploadID: nonExistendUploadID}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Expected error \"%v\" but found \"%v\"", expectedErr, err)
	}

	// Put object part with valid arguments
	pi, err := obj.PutObjectPart(GlobalContext, bucketName, objectName, uploadID.UploadID, partNumber, reader, ObjectOptions{})
	if err != nil {
		t.Fatalf("Unexpected error \"%v\"", err)
	}
	if pi.PartNumber != partNumber {
		t.Fatalf("Expectied part number %v but found %v", partNumber, pi.PartNumber)
	}
	expectedSize := int64(len(contentBytes))
	if pi.Size != expectedSize {
		t.Fatalf("Expectied size %v but found %v", expectedSize, pi.Size)
	}
}

func TestPANFSSeveralNewMultipartUpload(t *testing.T) {
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)

	res1, _ := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{})
	res2, _ := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{})

	checkMultipartTempFolder(t, obj, res1, 2, 1)
	checkMultipartTempFolder(t, obj, res2, 2, 0)
}

func checkMultipartTempFolder(t *testing.T, obj ObjectLayer, res *NewMultipartUploadResult, mapLen int, tmpFolderNumDelta uint64) {
	objPanFS := obj.(*PANFSObjects)
	if len(objPanFS.multipartTmpFolder) != mapLen {
		t.Fatalf("Expectied size %v but found %v", mapLen, len(objPanFS.multipartTmpFolder))
	}
	if mapLen == 0 {
		return
	}
	testPath := pathJoin(panfsS3TmpDir, strconv.FormatUint(objPanFS.currentTmpFolder-tmpFolderNumDelta, 10))
	if path, ok := objPanFS.multipartTmpFolder[res.UploadID]; !ok {
		t.Fatalf("Expectied value in multipartTmpFolder for key %s", res.UploadID)
	} else if !strings.Contains(path, testPath) {
		t.Fatalf("Path to multipart temp folder doesn't contain %s: %s", testPath, path)
	}
}
