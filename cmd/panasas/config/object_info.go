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

package config

import (
	"encoding/json"
	"io/fs"
	"time"
)

// ObjectInfo - object information returned by Panasas config agent
type ObjectInfo struct {
	ID         string            `json:"id"`
	Metadata   map[string]string `json:"metadata"`
	Namespace  NamespaceInfo     `json:"namespace"`
	ChangedAt  time.Time         `json:"timestamp"`
	ByteLength int64
}

// Name - return name of the object
func (poi *ObjectInfo) Name() string {
	return poi.ID
}

// Size - return object's byte length
func (poi *ObjectInfo) Size() int64 {
	return poi.ByteLength
}

// Mode - return the file mode bits for the object. The permissions are not
// supported so the value will be 0.
func (poi *ObjectInfo) Mode() fs.FileMode {
	return fs.FileMode(0)
}

// ModTime - returns object modification time
func (poi *ObjectInfo) ModTime() time.Time {
	return poi.ChangedAt
}

// IsDir - returns boolean to indicate if the object is a directory
func (poi *ObjectInfo) IsDir() bool {
	return false
}

// Sys - return pointer to some internal data
func (poi *ObjectInfo) Sys() any {
	return nil
}

func parseObjectInfo(JSONData string) (*ObjectInfo, error) {
	var oi ObjectInfo

	if err := json.Unmarshal([]byte(JSONData), &oi); err != nil {
		return nil, err
	}
	return &oi, nil
}
