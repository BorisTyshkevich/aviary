//go:build windows

package preparation

import (
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Jobs are kept alive until the process tree is terminated, including when
// the leader exits before descendants. The process starts suspended so it
// cannot spawn a child before it joins the job.
var windowsJobs sync.Map // map[*exec.Cmd]windows.Handle

type jobAccounting struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

// Match JOBOBJECT_BASIC_ACCOUNTING_INFORMATION from winnt.h. A wrong offset
// could mistake a live descendant for an empty job and publish mutable files.
var _ [48 - unsafe.Sizeof(jobAccounting{})]byte
var _ [unsafe.Sizeof(jobAccounting{}) - 48]byte
var _ [40 - unsafe.Offsetof(jobAccounting{}.ActiveProcesses)]byte
var _ [unsafe.Offsetof(jobAccounting{}.ActiveProcesses) - 40]byte

func startProcess(cmd *exec.Cmd) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	_, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)))
	if err != nil {
		_ = windows.CloseHandle(job)
		return err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_SUSPENDED}
	if err := cmd.Start(); err != nil {
		_ = windows.CloseHandle(job)
		return err
	}
	assigned := false
	cleanup := func() {
		if assigned {
			_ = windows.TerminateJobObject(job, 1)
		} else {
			_ = cmd.Process.Kill()
		}
		_ = windows.CloseHandle(job)
		_ = cmd.Wait()
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		cleanup()
		return err
	}
	err = windows.AssignProcessToJobObject(job, process)
	_ = windows.CloseHandle(process)
	if err != nil {
		cleanup()
		return err
	}
	assigned = true
	windowsJobs.Store(cmd, job)
	if err := resumePrimaryThread(uint32(cmd.Process.Pid)); err != nil {
		windowsJobs.Delete(cmd)
		cleanup()
		return err
	}
	return nil
}

func resumePrimaryThread(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(snapshot) }()
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err := windows.Thread32First(snapshot, &entry); err != nil {
		return err
	}
	for {
		if entry.OwnerProcessID == pid {
			thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if err != nil {
				return err
			}
			previous, err := windows.ResumeThread(thread)
			_ = windows.CloseHandle(thread)
			if err != nil {
				return err
			}
			if previous != 1 {
				return ErrUnavailable
			}
			return nil
		}
		if err := windows.Thread32Next(snapshot, &entry); err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				return ErrUnavailable
			}
			return err
		}
	}
}

func killProcessTree(cmd *exec.Cmd) error {
	value, ok := windowsJobs.LoadAndDelete(cmd)
	if !ok {
		return ErrUnavailable
	}
	job := value.(windows.Handle)
	defer func() { _ = windows.CloseHandle(job) }()
	if err := windows.TerminateJobObject(job, 1); err != nil {
		return err
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var accounting jobAccounting
		err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&accounting)), uint32(unsafe.Sizeof(accounting)), nil)
		if err != nil {
			return err
		}
		if accounting.ActiveProcesses == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrUnavailable
		}
		time.Sleep(5 * time.Millisecond)
	}
}
