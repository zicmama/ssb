// +build !darwin

package neterr

import "syscall"

func isFailingSyscall(err error) bool {
	if err == syscall.ECONNRESET || err == syscall.EPIPE {
		return true
	}
	return false
}
