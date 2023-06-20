package filelock

type Lock interface {
	Lock() error
	TryLock() error
	Unlock() error
}
