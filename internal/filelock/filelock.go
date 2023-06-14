package filelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var (
	ErrorNotLocked        = errors.New("not locked")
	ErrorLocked           = errors.New("already locked")
	ErrorDirNotExists     = errors.New("directory of file not exists")
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
	stat, err := os.Stat(filepath.Dir(path))
	if (err != nil && os.IsNotExist(err)) || !stat.IsDir() {
		return nil, ErrorDirNotExists
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

	var err error
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
			l.mutex.Unlock()
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
