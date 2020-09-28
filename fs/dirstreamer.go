package fs

import (
	"context"
	"path/filepath"
	"sync"
	"syscall"

	ffs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type sdfsDirStream struct {
	marker    string
	path      string
	ctx       context.Context
	mu        sync.Mutex
	nextEntry string
}

// NewsdfsDirStream open a directory for reading as a DirStream
func NewsdfsDirStream(ctx context.Context, name string) (ffs.DirStream, syscall.Errno) {
	_, err := con.Stat(ctx, name)
	if err != nil {
		return nil, ToErrno(err)
	}

	ds := &sdfsDirStream{
		path:   name,
		marker: "",
		ctx:    ctx,
	}

	if err := ds.load(); err != 0 {
		ds.Close()
		return nil, err
	}
	return ds, ffs.OK
}

func (ds *sdfsDirStream) Close() {

}

func (ds *sdfsDirStream) HasNext() bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return len(ds.marker) > 0
}

func (ds *sdfsDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	fi, err := con.GetAttr(ds.ctx, filepath.Join(ds.path, ds.nextEntry))
	if err != nil {
		return fuse.DirEntry{}, ToErrno(err)
	}
	result := fuse.DirEntry{
		Ino: uint64(fi.Dev),

		Mode: uint32(fi.Mode),
		Name: ds.nextEntry,
	}
	return result, ds.load()
}

func (ds *sdfsDirStream) load() syscall.Errno {
	marker, fi, err := con.ListDir(ds.ctx, ds.path, ds.marker, true, 1)
	if err != nil {
		return ToErrno(err)
	}
	ds.marker = marker
	if len(fi) == 0 {
		ds.marker = ""
		ds.nextEntry = ""
	} else {
		ds.marker = marker
		ds.nextEntry = fi[0].FileName
	}
	return ffs.OK
}
