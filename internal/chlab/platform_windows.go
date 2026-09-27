//go:build windows

package chlab

import (
	"os"

	"golang.org/x/sys/windows"
)

func acquireServiceLock(file *os.File) error {
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
}

func closeServiceLock(file *os.File) error {
	_ = windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &windows.Overlapped{})
	return file.Close()
}

func freeDiskAt(path string) (uint64, error) {
	widePath, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var available uint64
	if err := windows.GetDiskFreeSpaceEx(widePath, &available, nil, nil); err != nil {
		return 0, err
	}
	return available, nil
}
