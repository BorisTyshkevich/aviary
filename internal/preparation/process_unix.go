//go:build !windows

package preparation

import (
	"errors"
	"os/exec"
	"syscall"
)

func startProcess(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd.Start()
}

func killProcessTree(cmd *exec.Cmd) error {
	if cmd.Process != nil {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	}
	return nil
}
