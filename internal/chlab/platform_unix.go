//go:build !windows

package chlab

import (
	"os"
	"syscall"
)

func acquireServiceLock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func closeServiceLock(file *os.File) error {
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return file.Close()
}

func freeDiskAt(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
