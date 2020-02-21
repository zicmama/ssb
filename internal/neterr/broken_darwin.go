package neterr

import "syscall"

// on darwin, the error when using a closed socket is sometimes EPROTOTYPE
// see: http://erickt.github.io/blog/2014/11/19/adventures-in-debugging-a-potential-osx-kernel-bug/

func isFailingSyscall(err error) bool {
	if err == syscall.ECONNRESET || err == syscall.EPIPE || err == syscall.EPROTOTYPE {
		return true
	}
	return false
}
