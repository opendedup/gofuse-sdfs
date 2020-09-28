// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"log"
	"sync"
	"time"
	"unsafe"

	//	"time"

	"syscall"

	ffs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewsdfsFile creates a FileHandle out of a file descriptor. All
// operations are implemented.
func NewsdfsFile(fd int64, path string) ffs.FileHandle {
	return &sdfsFile{fd: fd, path: path}
}

type sdfsFile struct {
	mu   sync.Mutex
	fd   int64
	path string
}

var _ = (ffs.FileHandle)((*sdfsFile)(nil))
var _ = (ffs.FileReleaser)((*sdfsFile)(nil))
var _ = (ffs.FileGetattrer)((*sdfsFile)(nil))
var _ = (ffs.FileReader)((*sdfsFile)(nil))
var _ = (ffs.FileWriter)((*sdfsFile)(nil))
var _ = (ffs.FileFlusher)((*sdfsFile)(nil))
var _ = (ffs.FileFsyncer)((*sdfsFile)(nil))
var _ = (ffs.FileSetattrer)((*sdfsFile)(nil))

func futimens(fd int, times *[2]syscall.Timespec) (err error) {
	_, _, e1 := syscall.Syscall6(syscall.SYS_UTIMENSAT, uintptr(fd), 0, uintptr(unsafe.Pointer(times)), uintptr(0), 0, 0)
	if e1 != 0 {
		err = syscall.Errno(e1)
	}
	return
}

func setBlocks(out *fuse.Attr) {
	if out.Blksize > 0 {
		return
	}

	out.Blksize = 4096
	pages := (out.Size + 4095) / 4096
	out.Blocks = pages * 8
}

func (f *sdfsFile) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {

	rs, err := con.Read(ctx, f.fd, off, int32(len(buf)))
	copy(buf, rs)
	if err != nil {
		log.Printf("error %v \n", err)
		return nil, ToErrno(err)
	}
	r := fuse.ReadResultData(rs)
	return r, ffs.OK
}

func (f *sdfsFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {

	err := con.Write(ctx, f.fd, data, off, int32(len(data)))
	if err != nil {
		return 0, ToErrno(err)
	}
	return uint32(len(data)), ffs.OK
}

func (f *sdfsFile) Release(ctx context.Context) syscall.Errno {

	if f.fd != -1 {
		err := con.Release(ctx, f.fd)
		f.fd = -1
		return ToErrno(err)
	}
	return syscall.EBADF
}

func (f *sdfsFile) Flush(ctx context.Context) syscall.Errno {

	err := con.Flush(ctx, f.path, f.fd)
	return ToErrno(err)
}

func (f *sdfsFile) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {

	r := ffs.ToErrno(con.Fsync(ctx, f.path, f.fd))

	return r
}

func (f *sdfsFile) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if m, ok := in.GetMode(); ok {
		if err := con.Chmod(ctx, f.path, int32(m)); err != nil {
			return ToErrno(err)
		}
	}

	uid, uok := in.GetUID()
	gid, gok := in.GetGID()
	if uok || gok {
		suid := -1
		sgid := -1
		if uok {
			suid = int(uid)
		}
		if gok {
			sgid = int(gid)
		}
		if err := con.Chown(ctx, f.path, int32(sgid), int32(suid)); err != nil {
			return ToErrno(err)
		}
	}

	mtime, mok := in.GetMTime()
	atime, aok := in.GetATime()

	if mok || aok {

		ap := &atime
		mp := &mtime
		if !aok {
			ap = nil
		}
		if !mok {
			mp = nil
		}
		at := fuse.UtimeToTimespec(ap).Sec
		mt := fuse.UtimeToTimespec(mp).Sec

		if err := con.Utime(ctx, f.path, at, mt); err != nil {
			return ffs.ToErrno(err)
		}
	}

	if sz, ok := in.GetSize(); ok {
		if err := con.Truncate(ctx, f.path, int64(sz)); err != nil {
			return ffs.ToErrno(err)
		}
	}

	fi, err := con.GetAttr(ctx, f.path)
	if err != nil {
		return ToErrno(err)
	}
	atim := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctim := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtim := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	out.SetTimes(&atim, &mtim, &ctim)
	out.Size = uint64(fi.Size)
	out.Blocks = uint64(fi.Size / 512)
	out.Owner.Uid = uint32(fi.Uid)
	out.Owner.Gid = uint32(fi.Gid)
	out.Mode = uint32(fi.Mode)
	out.Blksize = 512

	return ffs.OK
}

func (f *sdfsFile) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	fi, err := con.GetAttr(ctx, f.path)
	if err != nil {
		return ToErrno(err)
	}
	atime := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctime := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtime := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	a.SetTimes(&atime, &mtime, &ctime)
	a.Size = uint64(fi.Size)
	a.Blocks = uint64(fi.Size / 512)
	a.Owner.Uid = uint32(fi.Uid)
	a.Owner.Gid = uint32(fi.Gid)
	a.Mode = uint32(fi.Mode)
	a.Blksize = 512
	return ffs.OK
}
