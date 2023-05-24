//go:build !panasas

package cmd

import (
	"os"
	"time"
)

func getBirthTime(fi os.FileInfo) time.Time {
	return fi.ModTime()
}
