package fs

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"time"

	ffs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	spb "github.com/opendedup/sdfs-client-go/api"
	sapi "github.com/opendedup/sdfs-client-go/sdfs"
)

type sdfsRoot struct {
	sdfsNode
	rootPath string
	rootDev  uint64
}

type sdfsNode struct {
	ffs.Inode
}

var con *spb.SdfsConnection = nil

var _ = (ffs.NodeStatfser)((*sdfsNode)(nil))
var _ = (ffs.NodeStatfser)((*sdfsNode)(nil))
var _ = (ffs.NodeGetattrer)((*sdfsNode)(nil))
var _ = (ffs.NodeGetxattrer)((*sdfsNode)(nil))
var _ = (ffs.NodeSetxattrer)((*sdfsNode)(nil))
var _ = (ffs.NodeRemovexattrer)((*sdfsNode)(nil))
var _ = (ffs.NodeListxattrer)((*sdfsNode)(nil))
var _ = (ffs.NodeOpener)((*sdfsNode)(nil))
var _ = (ffs.NodeCopyFileRanger)((*sdfsNode)(nil))
var _ = (ffs.NodeLookuper)((*sdfsNode)(nil))
var _ = (ffs.NodeOpendirer)((*sdfsNode)(nil))
var _ = (ffs.NodeReaddirer)((*sdfsNode)(nil))
var _ = (ffs.NodeMkdirer)((*sdfsNode)(nil))
var _ = (ffs.NodeMknoder)((*sdfsNode)(nil))
var _ = (ffs.NodeSymlinker)((*sdfsNode)(nil))
var _ = (ffs.NodeUnlinker)((*sdfsNode)(nil))
var _ = (ffs.NodeRmdirer)((*sdfsNode)(nil))
var _ = (ffs.NodeRenamer)((*sdfsNode)(nil))

func (n *sdfsNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {

	fi, err := con.GetXAttr(ctx, attr, n.path())
	if err != nil {
		return uint32(0), ToErrno(err)
	}
	sz := copy(dest, fi)
	return uint32(sz), ffs.OK
}

func (n *sdfsNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	s := string(data)
	err := con.SetXAttr(ctx, attr, s, n.path())
	if err != nil {
		return ToErrno(err)
	}
	return ffs.OK
}

func (n *sdfsNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	err := con.RemoveXAttr(ctx, attr, n.path())
	if err != nil {
		return ToErrno(err)
	}
	return ffs.OK
}

func (n *sdfsNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	fi, err := con.Stat(ctx, n.path())
	if err != nil {
		return uint32(0), ToErrno(err)
	}
	offset := 0
	for _, v := range fi.FileAttributes {
		kb := []byte(v.Key)
		copy(dest[offset:], kb)

		offset += len(kb) + 1
	}

	return uint32(offset), ffs.OK
}

func (n *sdfsNode) CopyFileRange(ctx context.Context, fhIn ffs.FileHandle,
	offIn uint64, out *ffs.Inode, fhOut ffs.FileHandle, offOut uint64,
	len uint64, flags uint64) (uint32, syscall.Errno) {
	lfIn, ok := fhIn.(*sdfsFile)
	if !ok {
		return 0, syscall.ENOTSUP
	}
	lfOut, ok := fhOut.(*sdfsFile)
	if !ok {
		return 0, syscall.ENOTSUP
	}

	signedOffIn := int64(offIn)
	signedOffOut := int64(offOut)
	count, err := con.CopyExtent(ctx, lfIn.path, lfOut.path, signedOffIn, signedOffOut, int64(len))
	if err != nil {
		return 0, ToErrno(err)
	}
	return uint32(count), ffs.OK
}

func (n *sdfsNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	fi, err := con.StatFS(ctx)
	if err != nil {
		return ToErrno(err)
	}
	out.Bavail = uint64(fi.Bfree)
	out.Bfree = uint64(fi.Bfree)
	out.Blocks = uint64(fi.Blocks)
	out.Bsize = uint32(fi.Bsize)
	out.NameLen = uint32(fi.Namelen)
	return ffs.OK
}

func (r *sdfsRoot) Getattr(ctx context.Context, f ffs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fi, err := con.GetAttr(ctx, r.path())
	if err != nil {
		return ToErrno(err)
	}
	atime := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctime := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtime := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	out.SetTimes(&atime, &mtime, &ctime)
	out.Size = uint64(fi.Size)
	out.Blocks = uint64(fi.Size / 512)
	out.Owner.Uid = uint32(fi.Uid)
	out.Owner.Gid = uint32(fi.Gid)
	out.Mode = uint32(fi.Mode)
	out.Blksize = 512
	return ffs.OK
}

func (n *sdfsNode) root() *sdfsRoot {
	return n.Root().Operations().(*sdfsRoot)
}

func (n *sdfsNode) path() string {
	path := n.Path(n.Root())
	return filepath.Join(n.root().rootPath, path)
}

func (n *sdfsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*ffs.Inode, syscall.Errno) {

	p := filepath.Join(n.path(), name)

	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		return nil, ToErrno(err)
	}
	ToStat(fi, out)
	node := &sdfsNode{}
	ch := n.NewInode(ctx, node, n.root().idFromStat(fi))
	return ch, 0
}

//ToStat turns a fileinfo into a stat
func ToStat(fi *sapi.Stat, out *fuse.EntryOut) {
	atime := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctime := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtime := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	out.SetTimes(&atime, &mtime, &ctime)
	out.Size = uint64(fi.Size)
	out.Blocks = uint64(fi.Size / 512)
	out.Owner.Uid = uint32(fi.Uid)
	out.Owner.Gid = uint32(fi.Gid)
	out.Mode = uint32(fi.Mode)
	out.Blksize = 512
	out.Rdev = 0
}

//ToAttr turns stat into attr
func ToAttr(fi *sapi.Stat, out *fuse.Attr) {
	atime := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctime := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtime := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	out.SetTimes(&atime, &mtime, &ctime)
	out.Size = uint64(fi.Size)
	out.Blocks = uint64(fi.Size / 512)
	out.Owner.Uid = uint32(fi.Uid)
	out.Owner.Gid = uint32(fi.Gid)
	out.Mode = uint32(fi.Mode)
	out.Blksize = 512
	out.Rdev = 0
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
func (n *sdfsNode) preserveOwner(ctx context.Context, path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return nil
	}
	err := con.Chown(ctx, path, int32(caller.Uid), int32(caller.Gid))
	if err != nil {
		return ToErrno(err)
	}
	return ffs.OK
}

func (n *sdfsNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*ffs.Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := con.MkNod(ctx, p, int32(mode), int32(rdev))
	if err != nil {
		return nil, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		return nil, ToErrno(err)
	}
	ToAttr(fi, &out.Attr)

	node := &sdfsNode{}
	ch := n.NewInode(ctx, node, n.root().idFromStat(fi))

	return ch, 0
}

func (n *sdfsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*ffs.Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := con.MkDir(ctx, p, int32(mode))
	if err != nil {
		return nil, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		con.RmDir(ctx, p)
		return nil, ToErrno(err)
	}

	ToAttr(fi, &out.Attr)

	node := &sdfsNode{}
	ch := n.NewInode(ctx, node, n.root().idFromStat(fi))

	return ch, 0
}

func (n *sdfsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	p := filepath.Join(n.path(), name)
	err := con.RmDir(ctx, p)
	if err != nil {
		return ToErrno(err)
	}
	return ffs.OK
}

func (n *sdfsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	p := filepath.Join(n.path(), name)
	err := con.Unlink(ctx, p)
	if err != nil {
		return ToErrno(err)
	}
	return ffs.OK
}

func tosdfsNode(op ffs.InodeEmbedder) *sdfsNode {
	if r, ok := op.(*sdfsRoot); ok {
		return &r.sdfsNode
	}
	return op.(*sdfsNode)
}

func (n *sdfsNode) Rename(ctx context.Context, name string, newParent ffs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newParentsdfs := tosdfsNode(newParent)
	/*
		if flags&ffs.RENAME_EXCHANGE != 0 {
			return n.renameExchange(name, newParentsdfs, newName)
		}
	*/

	p1 := filepath.Join(n.path(), name)
	p2 := filepath.Join(newParentsdfs.path(), newName)

	err := con.Rename(ctx, p1, p2)
	return ToErrno(err)
}

func (r *sdfsRoot) idFromStat(st *sapi.Stat) ffs.StableAttr {
	return ffs.StableAttr{
		Mode: uint32(st.Mode),
		Gen:  1,
		Ino:  uint64(st.Dev),
	}
}

var _ = (ffs.NodeCreater)((*sdfsNode)(nil))

func (n *sdfsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *ffs.Inode, fh ffs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := con.MkNod(ctx, p, int32(mode), 0)
	if err != nil {
		return nil, nil, 0, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		con.Unlink(ctx, p)
		return nil, nil, 0, ToErrno(err)
	}
	fd, err := con.Open(ctx, p, int32(flags))
	node := &sdfsNode{}
	ch := n.NewInode(ctx, node, n.root().idFromStat(fi))
	lf := NewsdfsFile(fd, p)
	ToAttr(fi, &out.Attr)
	return ch, lf, 0, 0
}

func (n *sdfsNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*ffs.Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := con.SymLink(ctx, p, target)
	if err != nil {
		return nil, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		con.Unlink(ctx, p)
		return nil, ToErrno(err)
	}
	node := &sdfsNode{}
	ch := n.NewInode(ctx, node, n.root().idFromStat(fi))

	return ch, 0
}

func (n *sdfsNode) Open(ctx context.Context, flags uint32) (fh ffs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	flags = flags &^ syscall.O_APPEND
	p := n.path()
	f, err := con.Open(ctx, p, int32(flags))
	if err != nil {
		return nil, 0, ToErrno(err)
	}
	lf := NewsdfsFile(f, p)
	return lf, 0, 0
}

func (n *sdfsNode) Opendir(ctx context.Context) syscall.Errno {
	p := n.path()
	_, err := con.Stat(ctx, p)
	if err != nil {
		return ToErrno(err)
	}
	return ffs.OK
}

func (n *sdfsNode) Readdir(ctx context.Context) (ffs.DirStream, syscall.Errno) {
	return NewsdfsDirStream(ctx, n.path())
}

func (n *sdfsNode) Getattr(ctx context.Context, f ffs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	p := n.path()

	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		return ToErrno(err)
	}
	atime := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctime := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtime := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	out.SetTimes(&atime, &mtime, &ctime)
	out.Size = uint64(fi.Size)
	out.Blocks = uint64(fi.Size / 512)
	out.Owner.Uid = uint32(fi.Uid)
	out.Owner.Gid = uint32(fi.Gid)
	out.Mode = uint32(fi.Mode)
	out.Blksize = 512
	return ffs.OK
}

var _ = (ffs.NodeSetattrer)((*sdfsNode)(nil))

func (n *sdfsNode) Setattr(ctx context.Context, f ffs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p := n.path()
	fsa, ok := f.(ffs.FileSetattrer)
	if ok && fsa != nil {
		fsa.Setattr(ctx, in, out)
	} else {
		if m, ok := in.GetMode(); ok {
			if err := con.Chmod(ctx, p, int32(m)); err != nil {
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
			if err := con.Chown(ctx, p, int32(sgid), int32(suid)); err != nil {
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

			if err := con.Utime(ctx, p, at, mt); err != nil {
				return ffs.ToErrno(err)
			}
		}

		if sz, ok := in.GetSize(); ok {
			if err := con.Truncate(ctx, p, int64(sz)); err != nil {
				return ffs.ToErrno(err)
			}
		}
	}

	fi, err := con.GetAttr(ctx, p)
	if err != nil {
		return ToErrno(err)
	}
	atime := time.Unix(0, fi.Atime*int64(time.Millisecond))
	ctime := time.Unix(0, fi.Ctim*int64(time.Millisecond))
	mtime := time.Unix(0, fi.Mtim*int64(time.Millisecond))
	out.SetTimes(&atime, &mtime, &ctime)
	out.Size = uint64(fi.Size)
	out.Blocks = uint64(fi.Size / 512)
	out.Owner.Uid = uint32(fi.Uid)
	out.Owner.Gid = uint32(fi.Gid)
	out.Mode = uint32(fi.Mode)
	out.Blksize = 512

	return ffs.OK
}

// NewsdfsRoot returns a root node for a sdfs file system whose
// root is at the given root. This node implements all NodeXxxxer
// operations available.
func NewsdfsRoot(root string, disableTrust bool, password string) (ffs.InodeEmbedder, error) {
	var err error
	spb.DisableTrust = disableTrust
	spb.Password = password
	con, err = spb.NewConnection(root)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fi, err := con.GetVolumeInfo(ctx)
	if err != nil {
		return nil, err
	}
	n := &sdfsRoot{
		rootPath: "/",
		rootDev:  uint64(fi.SerialNumber),
	}
	return n, nil
}
