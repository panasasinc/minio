package filelock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/minio/minio/internal/filelock"
)

type FilelockSuite struct {
	suite.Suite
}

func TestFilelockSuite(t *testing.T) {
	suite.Run(t, new(FilelockSuite))
}

func (suite *FilelockSuite) TestNew() {
	fl, err := filelock.New("tmp.lock")
	suite.NoError(err)
	suite.NotNil(fl)
}

func (suite *FilelockSuite) TestNewNoDir() {
	fl, err := filelock.New("not_exist/tmp.lock")
	suite.ErrorIs(err, filelock.ErrorDirNotExists)
	suite.Nil(fl)
}

func (suite *FilelockSuite) TestLock_LockUnlock() {
	fl, _ := filelock.New("tmp.lock")

	suite.NotPanics(fl.Lock)
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_LockUnlock() {
	fl, _ := filelock.New("tmp.lock")

	suite.True(fl.TryLock())
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestUnlock_NotLocked() {
	fl, _ := filelock.New("tmp.lock")

	suite.Panics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_AfterLock() {
	fl, _ := filelock.New("tmp.lock")

	suite.NotPanics(fl.Lock)
	suite.False(fl.TryLock())
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_AfterLock_DifferentInstances() {
	fl, _ := filelock.New("tmp.lock")
	fl1, _ := filelock.New("tmp.lock")

	suite.NotPanics(fl.Lock)
	suite.False(fl1.TryLock())
	suite.NotPanics(fl.Unlock)
}

func (suite *FilelockSuite) TestTryLock_AfterTryLock() {
	fl, _ := filelock.New("tmp.lock")

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
			fl, _ := filelock.New("tmp.lock")
			prepared <- struct{}{}
			<-start
			suite.NotPanics(fl.Lock)
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
	fl, _ := filelock.New("tmp.lock")

	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go func() {
			prepared <- struct{}{}
			<-start
			suite.NotPanics(fl.Lock)
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
	fl, _ := filelock.New("tmp.lock")
	fl1, _ := filelock.New("tmp.lock")

	fl.Lock()
	suite.False(fl1.TryLock())
	suite.False(fl1.TryLock())
	fl.Unlock()
}
