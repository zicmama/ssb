package neterr

import (
	"errors"
	"net"
	"os"
)

func IsConnBrokenErr(err error) bool {
	netErr := new(net.OpError)
	if errors.As(err, &netErr) {
		var sysCallErr = new(os.SyscallError)
		if errors.As(netErr.Err, &sysCallErr) {
			action := sysCallErr.Unwrap()
			if isFailingSyscall(action) {
				return true
			}
		}
		if netErr.Err.Error() == "use of closed network connection" {
			return true
		}
	}
	return false
}
