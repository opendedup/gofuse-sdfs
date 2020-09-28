package fs

import (
	"syscall"

	"github.com/hanwen/go-fuse/fuse"
	spb "github.com/opendedup/sdfs-client-go/api"
)

// ToErrno exhumes the syscall.Errno error from wrapped error values.
func ToErrno(err error) syscall.Errno {
	if err == nil {
		return syscall.Errno(0)
	}
	if e, ok := err.(*spb.SdfsError); ok {
		return syscall.Errno(e.ErrorCode)
	}
	return syscall.Errno(fuse.ENOSYS)
}
