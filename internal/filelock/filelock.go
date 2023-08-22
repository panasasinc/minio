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

package filelock

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

var (
	ErrorNotLocked        = errors.New("not locked")
	ErrorLocked           = errors.New("already locked")
	ErrorFileNotExists    = errors.New("lock is not exists")
	ErrorCannotCreateFind = errors.New("cannot create or find file")
)

const (
	blockFlags   = syscall.LOCK_EX
	noBlockFlags = syscall.LOCK_EX | syscall.LOCK_NB
)

type FileLock struct {
	path  string
	mutex *sync.RWMutex

	file *os.File
}

func New(path string) (*FileLock, error) {
	stat, err := os.Stat(path)
	if (err != nil && os.IsNotExist(err)) || stat.IsDir() {
		return nil, ErrorFileNotExists
	}
	return &FileLock{
		path:  path,
		mutex: &sync.RWMutex{},
	}, nil
}

func (l *FileLock) Lock() (err error) {
	l.mutex.Lock()
	defer func() {
		if err != nil {
			l.mutex.Unlock()
		}
	}()

	file, err := os.OpenFile(l.path, os.O_RDWR, 0o660)
	if err != nil {
		return ErrorCannotCreateFind
	}

	fd := int(file.Fd())
	for err = syscall.Flock(fd, blockFlags); err == syscall.EINTR; err = syscall.Flock(fd, blockFlags) {
	}
	if err != nil {
		return fmt.Errorf("syscall.Flock() failed on %q: %w", file.Name(), err)
	}
	l.file = file
	return nil
}

func (l *FileLock) TryLock() bool {
	if !l.mutex.TryLock() {
		return false
	}

	var err error = nil
	defer func() {
		if err != nil {
			l.mutex.Unlock()
		}
	}()

	file, err := os.OpenFile(l.path, os.O_RDWR, 0o660)
	if err != nil {
		return false
	}

	fd := int(file.Fd())
	if err = syscall.Flock(fd, noBlockFlags); err != nil {
		if err == syscall.EAGAIN || err == syscall.EACCES {
			_ = file.Close()
			return false
		}
		return false
	}
	l.file = file
	return true
}

func (l *FileLock) Unlock() {
	if l.file == nil {
		panic(ErrorNotLocked)
	}
	err := l.file.Close()
	if err != nil {
		panic(err)
	}
	l.file = nil
	l.mutex.Unlock()
}
