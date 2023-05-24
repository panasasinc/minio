//go:build panasas

package cmd

import (
	"os"
	"syscall"
	"time"
)

func getBirthTime(fi os.FileInfo) time.Time {
	if sys := fi.Sys(); sys != nil {
		ts := sys.(*syscall.Stat_t).Btimespec
		return time.Unix(int64(ts.Sec), int64(ts.Nsec))
	} else {
		return time.Time{}
	}
}
