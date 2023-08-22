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

package filelock_test

import (
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/minio/minio/internal/filelock"
)

type FilelockSuite struct {
	suite.Suite
	lockFile string
}

func (suite *FilelockSuite) SetupTest() {
	tmpDir := suite.T().TempDir()
	lockPath := path.Join(tmpDir, "tmp.lock")
	_, err := os.Create(lockPath)
	suite.lockFile = lockPath
	suite.NoError(err)
}

func TestFilelockSuite(t *testing.T) {
	suite.Run(t, new(FilelockSuite))
}

func (suite *FilelockSuite) TestNew() {
	fl, err := filelock.New(suite.lockFile)
	suite.NoError(err)
	suite.NotNil(fl)
}

func (suite *FilelockSuite) TestNewNoDir() {
	fl, err := filelock.New("not_exist/tmp.lock")
	suite.ErrorIs(err, filelock.ErrorFileNotExists)
	suite.Nil(fl)
}

func (suite *FilelockSuite) TestLock_LockUnlock() {
	fl, _ := filelock.New(suite.lockFile)
	err := fl.Lock()
	suite.NoError(err)
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_LockUnlock() {
	fl, _ := filelock.New(suite.lockFile)

	suite.True(fl.TryLock())
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestUnlock_NotLocked() {
	fl, _ := filelock.New(suite.lockFile)

	suite.Panics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_AfterLock() {
	fl, _ := filelock.New(suite.lockFile)

	suite.NoError(fl.Lock())
	suite.False(fl.TryLock())
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_AfterLock_DifferentInstances() {
	fl, _ := filelock.New(suite.lockFile)
	fl1, _ := filelock.New(suite.lockFile)

	suite.NoError(fl.Lock())
	suite.False(fl1.TryLock())
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_AfterTryLock() {
	fl, _ := filelock.New(suite.lockFile)

	suite.True(fl.TryLock())
	suite.False(fl.TryLock())
	fl.Unlock()
}

func (suite *FilelockSuite) TestLockConcurrent_SeveralInstances() {
	start := make(chan struct{})
	cnt := 10
	prepared := make(chan struct{}, cnt)
	wg := &sync.WaitGroup{}
	sleep := time.Millisecond * 100

	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go func() {
			fl, _ := filelock.New(suite.lockFile)
			prepared <- struct{}{}
			<-start
			suite.NoError(fl.Lock())
			time.Sleep(sleep)
			suite.NotPanics(fl.Unlock)
			wg.Done()
		}()
	}
	for i := 0; i < cnt; i++ {
		<-prepared
	}
	t := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t)
	suite.InDelta(sleep*time.Duration(cnt), elapsed, float64(time.Millisecond*100))
}

func (suite *FilelockSuite) TestLockConcurrent_OneInstance() {
	start := make(chan struct{})
	cnt := 10
	prepared := make(chan struct{}, cnt)
	wg := &sync.WaitGroup{}
	sleep := time.Millisecond * 100

	fl, _ := filelock.New(suite.lockFile)

	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go func() {
			prepared <- struct{}{}
			<-start
			suite.NoError(fl.Lock())
			time.Sleep(sleep)
			suite.NotPanics(fl.Unlock)
			wg.Done()
		}()
	}
	for i := 0; i < cnt; i++ {
		<-prepared
	}
	t := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t)
	suite.InDelta(sleep*time.Duration(cnt), elapsed, float64(time.Millisecond*100))
}

func (suite *FilelockSuite) TestTwoInstance_LockTryLock() {
	fl, _ := filelock.New(suite.lockFile)
	fl1, _ := filelock.New(suite.lockFile)

	fl.Lock()
	suite.False(fl1.TryLock())
	suite.False(fl1.TryLock())
	fl.Unlock()
}
