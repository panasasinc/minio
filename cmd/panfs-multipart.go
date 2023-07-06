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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/filelock"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/trie"
)

const (
	panbgAppendsCleanupInterval = 10 * time.Minute
)

// Returns EXPORT/bucket/.s3/multipart/SHA256/UPLOADID
func (fs *PANFSObjects) getUploadIDDir(bucketPath, object, uploadID string) string {
	return pathJoin(bucketPath, panfsS3MultipartDir, getSHA256Hash([]byte(object)), uploadID)
}

// getMultipartLockFile returns path to the lock file based on bucket, object and uploadID
// Returns EXPORT/bucket/.s3/multipart/SHA256/uploadid.lock
func (fs *PANFSObjects) getMultipartLockFile(bucketPath, object, uploadID string) string {
	return pathJoin(fs.getMultipartSHADir(bucketPath, object), uploadID+".lock")
}

// Returns EXPORT/bucket/.s3/multipart/SHA256
func (fs *PANFSObjects) getMultipartSHADir(bucketPath, object string) string {
	return pathJoin(bucketPath, panfsS3MultipartDir, getSHA256Hash([]byte(object)))
}

// getUploadIDFilePartsPath to the file containing info about current multipart upload progress
// Returns EXPORT/bucket/.s3/multipart/SHA256/UPLOADID/mparts.json
func (fs *PANFSObjects) getUploadIDFilePartsPath(bucketPath, object, uploadID string) string {
	return pathJoin(fs.getUploadIDDir(bucketPath, object, uploadID), "mparts.json")
}

func (fs *PANFSObjects) getBackgroundAppendsDir(bucketPath string) string {
	return pathJoin(bucketPath, panfsS3TmpDir, bgAppendsDirName)
}

// getObjectBackgroundAppendPath returns path to the requested object according to upload id and bucket path
// Returns EXPORT/bucket/.s3/tmp/bg-appends/UPLOADID
func (fs *PANFSObjects) getObjectBackgroundAppendPath(bucketPath, object, uploadID string) string {
	return pathJoin(fs.getBackgroundAppendsDir(bucketPath), uploadID)
}

// getPanFSMultipartAppendFile returns and initialize panfsAppendFile struct
func (fs *PANFSObjects) getPanFSMultipartAppendFile(bucketPath, object, uploadID string) (*panfsAppendFile, error) {
	flock, err := filelock.New(fs.getMultipartLockFile(bucketPath, object, uploadID))
	if err != nil {
		return nil, err
	}

	appendFile := &panfsAppendFile{
		filePath: fs.getObjectBackgroundAppendPath(bucketPath, object, uploadID),
		flock:    flock,
	}
	return appendFile, nil
}

// Returns partNumber.etag
func (fs *PANFSObjects) encodePartFile(partNumber int, etag string, actualSize int64) string {
	return fmt.Sprintf("%.5d.%s.%d", partNumber, etag, actualSize)
}

// Returns partNumber and etag
func (fs *PANFSObjects) decodePartFile(name string) (partNumber int, etag string, actualSize int64, err error) {
	result := strings.Split(name, ".")
	if len(result) != 3 {
		return 0, "", 0, errUnexpected
	}
	partNumber, err = strconv.Atoi(result[0])
	if err != nil {
		return 0, "", 0, errUnexpected
	}
	actualSize, err = strconv.ParseInt(result[2], 10, 64)
	if err != nil {
		return 0, "", 0, errUnexpected
	}
	return partNumber, result[1], actualSize, nil
}

// Appends parts to an appendFile sequentially.
func (fs *PANFSObjects) backgroundAppend(ctx context.Context, bucket, object, uploadID string, needFlock bool) {
	logger.GetReqInfo(ctx).AppendTags("uploadID", uploadID)
	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return
	}
	fs.appendFileMapMu.Lock()
	file := fs.appendFileMap[uploadID]
	if file == nil {
		file, err = fs.getPanFSMultipartAppendFile(bucketPath, object, uploadID)
		if err != nil {
			fs.appendFileMapMu.Unlock()
			return
		}
		fs.appendFileMap[uploadID] = file
	}
	fs.appendFileMapMu.Unlock()

	// No need to take a lock when bgAppend called from CompleteMultipartUpload which has been already taken the lock
	if needFlock {
		if !file.flock.TryLock() {
			return
		}
		defer file.flock.Unlock()
	}

	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)

	// Read initial mod time of upload dir
	prevUploadIDDirStat, err := fsStatDir(ctx, uploadIDDir)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}
	fsParts := fs.readMPartUploadParts(bucketPath, object, uploadID)
	// Since we append sequentially nextPartNumber will always be len(fsParts.Appended)+1
	initialAppendedLen := len(fsParts.Parts)
	nextPartNumber := initialAppendedLen + 1

	for {
		entries, err := readDir(uploadIDDir)
		if err != nil {
			logger.GetReqInfo(ctx).AppendTags("uploadIDDir", uploadIDDir)
			logger.LogIf(ctx, err)
			break
		}
		sort.Strings(entries)

		for _, entry := range entries {
			if entry == fs.metaJSONFile {
				continue
			}
			partNumber, etag, actualSize, err := fs.decodePartFile(entry)
			if err != nil {
				// Skip part files whose name don't match expected format. These could be backend filesystem specific files.
				continue
			}
			if partNumber < nextPartNumber {
				// Part already appended.
				continue
			}
			if partNumber > nextPartNumber {
				// Required part number is not yet uploaded.
				break
			}

			partPath := pathJoin(uploadIDDir, entry)
			err = xioutil.AppendFile(file.filePath, partPath, globalFSOSync)
			if err != nil {
				reqInfo := logger.GetReqInfo(ctx).AppendTags("partPath", partPath)
				reqInfo.AppendTags("filepath", file.filePath)
				logger.LogIf(ctx, err)
				return
			}

			partInfo := PartInfo{PartNumber: partNumber, ETag: etag, ActualSize: actualSize}
			fsParts.Parts = append(fsParts.Parts, partInfo)
			file.parts = append(file.parts, partInfo)
			nextPartNumber++
		}

		uploadIDDirStat, err := fsStatDir(ctx, uploadIDDir)
		if err != nil {
			// Do not return at this step - some parts may be appended to the file but not added to meta
			break
		}
		if uploadIDDirStat.ModTime() == prevUploadIDDirStat.ModTime() {
			break
		}
		prevUploadIDDirStat = uploadIDDirStat
	}

	// No parts appended - just return. No need to update meta file
	if initialAppendedLen == len(fsParts.Parts) {
		return
	}

	// Update info about uploaded parts
	panfsPartsBytes, err := json.Marshal(fsParts)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}

	panfsPartsTmpPath := pathJoin(bucketPath, panfsS3TmpDir, mustGetUUID())
	defer func() {
		if err != nil {
			fsRemoveFile(ctx, panfsPartsTmpPath)
		}
	}()
	if err = os.WriteFile(panfsPartsTmpPath, panfsPartsBytes, 0o660); err != nil {
		logger.LogIf(ctx, err)
		return
	}
	if err = PanRenameFile(panfsPartsTmpPath, fs.getUploadIDFilePartsPath(bucketPath, object, uploadID)); err != nil {
		logger.LogIf(ctx, err)
		return
	}
}

// ListMultipartUploads - lists all the uploadIDs for the specified object.
// We do not support prefix based listing.
func (fs *PANFSObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, e error) {
	if err := checkListMultipartArgs(ctx, bucket, object, keyMarker, uploadIDMarker, delimiter, fs); err != nil {
		return result, toObjectErr(err)
	}
	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return result, toObjectErr(err)
	}

	if _, err := fsStatVolume(ctx, bucketPath); err != nil {
		return result, toObjectErr(err, bucket)
	}

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter
	result.NextKeyMarker = object
	result.UploadIDMarker = uploadIDMarker

	objectSHADir := fs.getMultipartSHADir(bucketPath, object)

	uploadIDs, err := readDir(objectSHADir)
	if err != nil {
		if err == errFileNotFound {
			result.IsTruncated = false
			return result, nil
		}
		logger.LogIf(ctx, err)
		return result, toObjectErr(err)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time. ModTime of panfs.json
	// is the creation time of the uploadID, hence we will use that.
	var uploads []MultipartInfo
	for _, uploadID := range uploadIDs {
		if !strings.HasSuffix(uploadID, "/") {
			continue
		}
		metaFilePath := pathJoin(objectSHADir, uploadID, fs.metaJSONFile)
		fi, err := fsStatFile(ctx, metaFilePath)
		if err != nil {
			return result, toObjectErr(err, bucket, object)
		}
		uploads = append(uploads, MultipartInfo{
			Object:    object,
			UploadID:  strings.TrimSuffix(uploadID, SlashSeparator),
			Initiated: fi.ModTime(),
		})
	}
	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (fs *PANFSObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, fs); err != nil {
		return nil, toObjectErr(err, bucket)
	}

	if err := dotS3PrefixCheck(bucket, object); err != nil {
		return nil, err
	}
	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return nil, toObjectErr(err)
	}

	if _, err := fsStatVolume(ctx, bucketPath); err != nil {
		return nil, toObjectErr(err, bucket)
	}

	uploadID := mustGetUUID()
	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)

	err = mkdirAll(uploadIDDir, 0o755)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	// Creates dir for background append procedure
	err = mkdirAll(fs.getBackgroundAppendsDir(bucketPath), 0o755)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	// Initialize panfs.json values.
	fsMeta := newPANFSMeta()
	fsMeta.Meta = opts.UserDefined

	fsMetaBytes, err := json.Marshal(fsMeta)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	if err = ioutil.WriteFile(pathJoin(uploadIDDir, fs.metaJSONFile), fsMetaBytes, 0o666); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	// create lock file for current multipart upload
	if _, err = os.Create(fs.getMultipartLockFile(bucketPath, object, uploadID)); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	return &NewMultipartUploadResult{UploadID: uploadID}, nil
}

// CopyObjectPart - similar to PutObjectPart but reads data from an existing
// object. Internally incoming data is written to '.minio.sys/tmp' location
// and safely renamed to '.minio.sys/multipart' for reach parts.
func (fs *PANFSObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, err error,
) {
	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return pi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	if err = dotS3PrefixCheck(srcBucket, srcObject, dstBucket, dstObject); err != nil {
		return pi, err
	}

	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject, fs); err != nil {
		return pi, toObjectErr(err)
	}

	partInfo, err := fs.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	return partInfo, nil
}

// PutObjectPart - reads incoming data until EOF for the part file on
// an ongoing multipart transaction. Internally incoming data is
// written to '<bucketPath>/.s3/tmp' location and safely renamed to
// '<bucketPath>/.s3/multipart' for reach parts.
func (fs *PANFSObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, e error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return pi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	data := r.Reader
	if err := checkPutObjectPartArgs(ctx, bucket, object, fs); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return pi, toObjectErr(err)
	}

	if _, err := fsStatVolume(ctx, bucketPath); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	// Validate input data size and it can never be less than -1.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err = fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return pi, toObjectErr(err, bucket, object)
	}

	tmpPartPath := pathJoin(bucketPath, panfsS3TmpDir, fs.nodeDataSerial, uploadID+"."+mustGetUUID()+"."+strconv.Itoa(partID))
	bytesWritten, err := fsCreateFile(ctx, tmpPartPath, data, data.Size())

	// Delete temporary part in case of failure. If
	// PutObjectPart succeeds then there would be nothing to
	// delete in which case we just ignore the error.
	defer fsRemoveFile(ctx, tmpPartPath)

	if err != nil {
		return pi, toObjectErr(err, panfsS3TmpDir, tmpPartPath)
	}

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		return pi, IncompleteBody{Bucket: bucket, Object: object}
	}

	etag := r.MD5CurrentHexString()

	if etag == "" {
		etag = GenETag()
	}

	partPath := pathJoin(uploadIDDir, fs.encodePartFile(partID, etag, data.ActualSize()))

	// Make sure not to create parent directories if they don't exist - the upload might have been aborted.
	if err = PanRenameFile(tmpPartPath, partPath); err != nil {
		return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
	}

	go fs.backgroundAppend(context.Background(), bucket, object, uploadID, true)

	fi, err := fsStatFile(ctx, partPath)
	if err != nil {
		return pi, toObjectErr(err, panfsS3MultipartDir, partPath)
	}
	return PartInfo{
		PartNumber:   partID,
		LastModified: fi.ModTime(),
		ETag:         etag,
		Size:         fi.Size(),
		ActualSize:   data.ActualSize(),
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
func (fs *PANFSObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	minfo := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	if err := checkListPartsArgs(ctx, bucket, object, fs); err != nil {
		return minfo, toObjectErr(err)
	}

	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return minfo, toObjectErr(err)
	}

	// Check if bucket exists
	if _, err := fsStatVolume(ctx, bucketPath); err != nil {
		return minfo, toObjectErr(err, bucket)
	}

	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)
	if _, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile)); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return minfo, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return minfo, toObjectErr(err, bucket, object)
	}

	fsMetaBytes, err := xioutil.ReadFile(pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return minfo, toObjectErr(err, bucket, object)
	}

	var fsMeta panfsMeta
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBytes, &fsMeta); err != nil {
		return minfo, toObjectErr(err, bucket, object)
	}

	minfo.UserDefined = fsMeta.Meta
	return minfo, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (fs *PANFSObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, e error) {
	if err := checkListPartsArgs(ctx, bucket, object, fs); err != nil {
		return result, toObjectErr(err)
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker

	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return result, toObjectErr(err)
	}

	// Check if bucket exists
	if _, err := fsStatVolume(ctx, bucketPath); err != nil {
		return result, toObjectErr(err, bucket)
	}

	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)
	if _, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile)); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return result, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return result, toObjectErr(err, bucket, object)
	}

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.LogIf(ctx, err)
		return result, toObjectErr(err, bucket)
	}

	partsMap := make(map[int]PartInfo)
	for _, entry := range entries {
		if entry == fs.metaJSONFile {
			continue
		}

		partNumber, currentEtag, actualSize, derr := fs.decodePartFile(entry)
		if derr != nil {
			// Skip part files whose name don't match expected format. These could be backend filesystem specific files.
			continue
		}

		entryStat, err := fsStatFile(ctx, pathJoin(uploadIDDir, entry))
		if err != nil {
			continue
		}

		currentMeta := PartInfo{
			PartNumber:   partNumber,
			ETag:         currentEtag,
			ActualSize:   actualSize,
			Size:         entryStat.Size(),
			LastModified: entryStat.ModTime(),
		}

		cachedMeta, ok := partsMap[partNumber]
		if !ok {
			partsMap[partNumber] = currentMeta
			continue
		}

		if currentMeta.LastModified.After(cachedMeta.LastModified) {
			partsMap[partNumber] = currentMeta
		}
	}

	var parts []PartInfo
	for _, partInfo := range partsMap {
		parts = append(parts, partInfo)
	}

	sort.Slice(parts, func(i int, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	i := 0
	if partNumberMarker != 0 {
		// If the marker was set, skip the entries till the marker.
		for _, part := range parts {
			i++
			if part.PartNumber == partNumberMarker {
				break
			}
		}
	}

	partsCount := 0
	for partsCount < maxParts && i < len(parts) {
		result.Parts = append(result.Parts, parts[i])
		i++
		partsCount++
	}
	if i < len(parts) {
		result.IsTruncated = true
		if partsCount != 0 {
			result.NextPartNumberMarker = result.Parts[partsCount-1].PartNumber
		}
	}

	rc, _, err := fsOpenFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile), 0)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return result, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return result, toObjectErr(err, bucket, object)
	}
	defer rc.Close()

	fsMetaBytes, err := ioutil.ReadAll(rc)
	if err != nil {
		return result, toObjectErr(err, bucket, object)
	}

	var fsMeta fsMetaV1
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBytes, &fsMeta); err != nil {
		return result, err
	}

	result.UserDefined = fsMeta.Meta
	return result, nil
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (fs *PANFSObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	var actualSize int64
	completed := false

	if err = checkCompleteMultipartArgs(ctx, bucket, object, fs); err != nil {
		return oi, toObjectErr(err)
	}

	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return oi, toObjectErr(err)
	}

	if _, err = fsStatVolume(ctx, bucketPath); err != nil {
		return oi, toObjectErr(err, bucket)
	}
	defer NSUpdated(bucket, object)
	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)

	handleStatMetaFileError := func(e error) error {
		// caller is responsible to check that e is not nil
		if e == errFileNotFound || e == errFileAccessDenied {
			return InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return toObjectErr(e, bucket, object)
	}

	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err = fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		return oi, handleStatMetaFileError(err)
	}

	// Most of the times appendFile would already be fully appended by now. We call fs.backgroundAppend()
	// to take care of the following corner case:
	// 1. The last PutObjectPart triggers go-routine fs.backgroundAppend, this go-routine has not started yet.
	// 2. Now CompleteMultipartUpload gets called which sees that lastPart is not appended and starts appending
	//    from the beginning
	fs.appendFileMapMu.Lock()
	file := fs.appendFileMap[uploadID]
	if file == nil {
		file, err = fs.getPanFSMultipartAppendFile(bucketPath, object, uploadID)
		if err != nil {
			fs.appendFileMapMu.Unlock()
			return oi, err
		}
		fs.appendFileMap[uploadID] = file
	}
	fs.appendFileMapMu.Unlock()

	// Take a lock before reading parts and do complete upload stuff
	if err = file.flock.Lock(); err != nil {
		return oi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
	}
	defer func() {
		if completed {
			file.flock.Unlock()
			fsRemoveFile(ctx, fs.getMultipartLockFile(bucketPath, object, uploadID))
			fs.appendFileMapMu.Lock()
			delete(fs.appendFileMap, uploadID)
			fs.appendFileMapMu.Unlock()
		} else {
			file.flock.Unlock()
		}
	}()

	// ensure that uploadId dir is still here
	_, err = fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		return oi, handleStatMetaFileError(err)
	}

	// ensure that part ETag is canonicalized to strip off extraneous quotes
	for i := range parts {
		parts[i].ETag = canonicalizeETag(parts[i].ETag)
	}

	fsMeta := panfsMeta{}

	// Allocate parts similar to incoming slice.
	fsMeta.Parts = make([]ObjectPartInfo, len(parts))

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.GetReqInfo(ctx).AppendTags("uploadIDDir", uploadIDDir)
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Create entries trie structure for prefix match
	entriesTrie := trie.NewTrie()
	for _, entry := range entries {
		entriesTrie.Insert(entry)
	}

	// Save consolidated actual size.
	var objectActualSize int64
	// Validate all parts and then commit to disk.
	for i, part := range parts {
		partFile := getPartFile(entriesTrie, part.PartNumber, part.ETag)
		if partFile == "" {
			return oi, InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
		}

		// Read the actualSize from the pathFileName.
		subParts := strings.Split(partFile, ".")
		actualSize, err = strconv.ParseInt(subParts[len(subParts)-1], 10, 64)
		if err != nil {
			return oi, InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
		}

		partPath := pathJoin(uploadIDDir, partFile)

		var fi os.FileInfo
		fi, err = fsStatFile(ctx, partPath)
		if err != nil {
			if err == errFileNotFound || err == errFileAccessDenied {
				return oi, InvalidPart{}
			}
			return oi, err
		}

		fsMeta.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			Size:       fi.Size(),
			ActualSize: actualSize,
		}

		// Consolidate the actual size.
		objectActualSize += actualSize

		if i == len(parts)-1 {
			break
		}

		// All parts except the last part has to be atleast 5MB.
		if !isMinAllowedPartSize(actualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   actualSize,
				PartETag:   part.ETag,
			}
		}
	}

	appendFallback := true // In case background-append did not append the required parts.

	fs.backgroundAppend(ctx, bucket, object, uploadID, false)

	fsParts := fs.readMPartUploadParts(bucketPath, object, uploadID)
	// Verify that appendFile has all the parts.
	if len(fsParts.Parts) == len(parts) {
		for i := range parts {
			if parts[i].ETag != fsParts.Parts[i].ETag {
				break
			}
			if parts[i].PartNumber != fsParts.Parts[i].PartNumber {
				break
			}
			if i == len(parts)-1 {
				appendFallback = false
			}
		}
	}

	if appendFallback {
		fsRemoveFile(ctx, file.filePath)
		fsRemoveFile(ctx, fs.getUploadIDFilePartsPath(bucketPath, object, uploadID))

		for _, part := range parts {
			partFile := getPartFile(entriesTrie, part.PartNumber, part.ETag)
			if partFile == "" {
				logger.LogIf(ctx, fmt.Errorf("%.5d.%s missing will not proceed",
					part.PartNumber, part.ETag))
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
					GotETag:    part.ETag,
				}
			}
			if err = xioutil.AppendFile(file.filePath, pathJoin(uploadIDDir, partFile), globalFSOSync); err != nil {
				logger.LogIf(ctx, err)
				return oi, toObjectErr(err)
			}
		}
	}

	// Hold write lock on the object.
	destLock := fs.NewNSLock(bucket, object)
	lkctx, err := destLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer destLock.Unlock(lkctx.Cancel)

	bucketMetaDir := pathJoin(bucketPath, panfsS3MetadataDir)
	fsMetaPath := pathJoin(bucketMetaDir, object)
	metaFile, err := fs.rwPool.Write(fsMetaPath)
	var freshFile bool
	if err != nil {
		if !errors.Is(err, errFileNotFound) {
			logger.LogIf(ctx, err)
			return oi, toObjectErr(err, bucket, object)
		}
		metaFile, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return oi, toObjectErr(err, bucket, object)
		}
		freshFile = true
	}
	defer metaFile.Close()
	defer func() {
		// Remove meta file when CompleteMultipart encounters
		// any error and it is a fresh file.
		//
		// We should preserve the `panfs.json` of any
		// existing object
		if err != nil && freshFile {
			tmpDir := pathJoin(bucketPath, panfsS3TmpDir, fs.nodeDataSerial)
			fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
		}
	}()

	// Read saved fs metadata for ongoing multipart.
	fsMetaBuf, err := xioutil.ReadFile(pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}
	err = json.Unmarshal(fsMetaBuf, &fsMeta)
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}
	// Save additional metadata.
	if fsMeta.Meta == nil {
		fsMeta.Meta = make(map[string]string)
	}

	fsMeta.Meta["etag"] = opts.UserDefined["etag"]
	if fsMeta.Meta["etag"] == "" {
		fsMeta.Meta["etag"] = getCompleteMultipartMD5(parts)
	}

	// Save consolidated actual size.
	fsMeta.Meta[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	if _, err = fsMeta.WriteTo(metaFile); err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	uid, gid := fs.getOwnerGroupIDs(ctx)
	err = panfsPublishFile(file.filePath, pathJoin(bucketPath, object), fs.defaultObjMode, fs.defaultDirMode, uid, gid)
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	// Purge multipart folders
	{
		fsTmpObjPath := pathJoin(bucketPath, panfsS3TmpDir, fs.nodeDataSerial, mustGetUUID())
		defer fsRemoveAll(ctx, fsTmpObjPath) // remove multipart temporary files in background.

		Rename(uploadIDDir, fsTmpObjPath)

		// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
		fsRemoveDir(ctx, fs.getMultipartSHADir(bucketPath, object))
	}

	fi, err := fsStatFile(ctx, pathJoin(bucketPath, object))
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	completed = true
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
//
// Implements S3 compatible Abort multipart API, slight difference is
// that this is an atomic idempotent operation. Subsequent calls have
// no affect and further requests to the same uploadID would not be
// honored.
func (fs *PANFSObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
	if err := checkAbortMultipartArgs(ctx, bucket, object, fs); err != nil {
		return err
	}
	bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		return toObjectErr(err) // Or just err?
	}

	if _, err := fsStatVolume(ctx, bucketPath); err != nil {
		return toObjectErr(err, bucket)
	}

	handleStatMetaFileError := func(e error) error {
		// caller is responsible to check that e is not nil
		if e == errFileNotFound || e == errFileAccessDenied {
			return InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return toObjectErr(e, bucket, object)
	}

	uploadIDDir := fs.getUploadIDDir(bucketPath, object, uploadID)
	_, err = fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		return handleStatMetaFileError(err)
	}

	fs.appendFileMapMu.Lock()
	file := fs.appendFileMap[uploadID]
	if file == nil {
		file, err = fs.getPanFSMultipartAppendFile(bucketPath, object, uploadID)
		if err != nil {
			fs.appendFileMapMu.Unlock()
			return err
		}
		fs.appendFileMap[uploadID] = file
	}
	fs.appendFileMapMu.Unlock()

	if err = file.flock.Lock(); err != nil {
		return InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
	}
	defer func() {
		if err == nil {
			file.flock.Unlock()
			// remove lock file
			fsRemoveFile(ctx, fs.getMultipartLockFile(bucketPath, object, uploadID))
			// Delete append file from memory only if no error occurred during mkdir and rename on next steps
			fs.appendFileMapMu.Lock()
			delete(fs.appendFileMap, uploadID)
			fs.appendFileMapMu.Unlock()
		} else {
			file.flock.Unlock()
		}
	}()

	// ensure that uploadId dir is still here
	_, err = fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		return handleStatMetaFileError(err)
	}
	// Purge multipart folders
	tempDir := fs.getTempDir(bucketPath)
	if err = MkdirAll(tempDir, 0o755); err != nil {
		return toObjectErr(err, bucket, object)
	}
	fsTmpPath := pathJoin(tempDir, MustGetUUID())
	defer fsRemoveAll(ctx, fsTmpPath)

	err = Rename(uploadIDDir, fsTmpPath)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	fsRemoveFile(ctx, file.filePath)
	return nil
}

// Return all uploads IDs with full path of each upload-id directory.
// Do not return an error as this is a lazy operation
func (fs *PANFSObjects) getAllUploadIDs(ctx context.Context) (result map[string]string) {
	result = make(map[string]string)

	buckets, err := fs.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return
	}
	for _, bucket := range buckets {
		bucketMPartDir := pathJoin(bucket.PanFSPath, panfsS3MultipartDir)
		objects, err := readDir(bucketMPartDir)
		if err != nil {
			continue
		}
		for _, object := range objects {
			uploadIDs, err := readDir(pathJoin(bucketMPartDir, object))
			if err != nil {
				continue
			}

			// Remove the trailing slash separator
			for i := range uploadIDs {
				uploadID := strings.TrimSuffix(uploadIDs[i], SlashSeparator)
				uploadIDDir := pathJoin(bucket.PanFSPath, panfsS3MultipartDir, object, uploadID)
				if _, err = fsStatDir(ctx, uploadIDDir); err == nil {
					result[uploadID] = uploadIDDir
				}
			}
		}
	}
	return
}

func (fs *PANFSObjects) cleanupStaleBackgroundAppendFiles(ctx context.Context) {
	fs.appendFileMapMu.Lock()
	appendFileMapIDsForDeletion := set.NewStringSet()
	for uploadID := range fs.appendFileMap {
		appendFileMapIDsForDeletion.Add(uploadID)
	}
	fs.appendFileMapMu.Unlock()

	buckets, err := fs.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return
	}

	appendFilesForDeletion := make(map[string]string)
	for _, bucket := range buckets {
		bucketAppendsDir := fs.getBackgroundAppendsDir(bucket.PanFSPath)
		appendFileNames, err := readDir(bucketAppendsDir)
		if err != nil {
			return
		}
		for _, entry := range appendFileNames {
			// Background append file names are the IDs of the uploads.
			appendFilePath := pathJoin(bucketAppendsDir, entry)
			appendFilesForDeletion[entry] = appendFilePath
		}
	}

	// WARNING:
	// If we first get a list of upload directories and then use it to
	// filter append files and entries in fs.appendFileMap, we create a
	// race condition where a new multi-part upload could be created after
	// listing the directories but before filtering the lists.
	//
	// To prevent such a race, get the list of upload directories as the
	// last step before filtering:
	uploadDirectories := fs.getAllUploadIDs(ctx)

	// Filter the items for deletion:
	for uploadID := range uploadDirectories {
		// If the upload directory related to the given upload
		// ID still exists, let's leave the append file.
		delete(appendFilesForDeletion, uploadID)

		appendFileMapIDsForDeletion.Remove(uploadID)
	}

	fs.appendFileMapMu.Lock()
	for _, uploadID := range appendFileMapIDsForDeletion.ToSlice() {
		delete(fs.appendFileMap, uploadID)
	}
	fs.appendFileMapMu.Unlock()

	go func() {
		for _, appendFilePath := range appendFilesForDeletion {
			fsRemoveFile(ctx, appendFilePath)
		}
	}()
}

// Removes multipart uploads if any older than `expiry` duration
// on all buckets for every `cleanupInterval`, this function is
// blocking and should be run in a go-routine.
func (fs *PANFSObjects) cleanupStaleUploads(ctx context.Context) {
	expiryUploadsTimer := time.NewTimer(globalAPIConfig.getStaleUploadsCleanupInterval())
	defer expiryUploadsTimer.Stop()

	bgAppendTmpCleaner := time.NewTimer(panbgAppendsCleanupInterval)
	defer bgAppendTmpCleaner.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-bgAppendTmpCleaner.C:
			fs.cleanupStaleBackgroundAppendFiles(ctx)
			bgAppendTmpCleaner.Reset(panbgAppendsCleanupInterval)

		case <-expiryUploadsTimer.C:
			expiry := globalAPIConfig.getStaleUploadsExpiry()
			now := time.Now()

			uploadIDs := fs.getAllUploadIDs(ctx)

			for uploadID, path := range uploadIDs {
				fi, err := fsStatDir(ctx, path)
				if err != nil {
					continue
				}
				if now.Sub(fi.ModTime()) > expiry {
					fsRemoveAll(ctx, path)
					// Remove upload ID parent directory if empty
					fsRemoveDir(ctx, filepath.Base(path))

					// Remove uploadID from the append file map and its corresponding temporary file
					fs.appendFileMapMu.Lock()
					bgAppend, ok := fs.appendFileMap[uploadID]
					if ok {
						_ = fsRemoveFile(ctx, bgAppend.filePath)
						delete(fs.appendFileMap, uploadID)
					}
					fs.appendFileMapMu.Unlock()
				}
			}

			// Reset for the next interval
			expiryUploadsTimer.Reset(globalAPIConfig.getStaleUploadsCleanupInterval())
		}
	}
}

// readMPartUploadParts returns panfsMultiParts structure containing list of appended parts.
func (fs *PANFSObjects) readMPartUploadParts(bucketPath, object, uploadID string) panfsMultiParts {
	fsParts := panfsMultiParts{}
	filePartsPath := fs.getUploadIDFilePartsPath(bucketPath, object, uploadID)
	fsMetaBuf, err := xioutil.ReadFile(filePartsPath)
	if err != nil {
		return panfsMultiParts{}
	}
	if err = json.Unmarshal(fsMetaBuf, &fsParts); err != nil {
		return panfsMultiParts{}
	}
	return fsParts
}
