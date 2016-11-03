/*
 * Copyright 2016 ClusterHQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package delta

import (
	"container/heap"
	"encoding/binary"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/kuba--/xattr"

	"github.com/ClusterHQ/go/dl/record"
)

/* delta generation takes an origin snapshot and a target snapshot that are
 * accessible via readonly mount points in the filesystem as an input. It then
 * traverses them in a depth first search algorithm and calls functions that
 * generate a sequence of instructions that allow reconstruction of the target
 * snapshot from the origin snapshot.
 *
 * It is analogous to a compiler in that it has a front end and a backend. At
 * present, there is only the first stage, but in the near future, we will have
 * 3 main stages:
 *
 *	* Stage 1	Parsing - We traverse the snapshots to determine which
 *			files are different while populating data structures.
 *
 *	* Stage 2	Link analysis- We make final determinations of whether
 *			things have bee renamed, are new or were unlinked. We
 *			also move things to a temporary directory before moving
 *			them to their final location in the situation where the
 *			directory has been unlinked, but a file has not.
 *
 *	* Stage 3	Delta Generation - We invoke callbacks to generate a
 *			delta that can be used to reconstruct the target
 *			snapshot from the origin snapshot.
 *
 * In a compiler, the parser generates an abstract syntax tree. Then type
 * checking is done for semantic analysis. This comprises the front end. Then
 * the backend does code generation. A compiler might have more stages, but we
 * do not. We also do not have an abstract syntax tree. It is still a WIP, but
 * we will build up in parsing and finalize in link analysis a dependency graph
 * that delta generation will use. This is required to handle certain cases.
 * For example, we run the following commands to generate an origin snapshot
 * and a target snapshot:
 *
 *	$ cd /example
 *	$ mkdir ./foo
 *	$ dd if=/dev/urandom of=./foo/bar bs=1m count=1000000
 *	$ # Make snapshot A and replicate A to another system (this will take a long time)
 *	$ mv ./foo/bar ./t
 *	$ rmdir ./foo
 *	$ mv ./t ./foo
 *	$ # Make snapshot B and replicate incremental of A to B.
 *
 * Thus, the origin snapshot contains:
 *
 *		./foo/
 *		./foo/bar  (inode number N)
 *
 * And, the target snapshot contains:
 *
 *		./foo (inode number N)
 *
 * Delta generation has the following constraints that must all be simultaneously satisfied:
 *
 *	* We cannot hardlink ./foo cannot be made before rmdir on ./foo
 *	* We cannot rmdir ./foo without unlinking ./foo/a
 *	* We cannot unlink ./foo/a before linking ./foo
 *
 * Similarly, we cannot rename ./foo/a to ./foo without an unlink. In either
 * case, we have a circular dependency graph. Such circularity can be
 * arbitrarily complex. If we design an algorithm that does not take them into
 * account, it is possible to perform denial of service attacks. One of stage
 * 2's responsibilities is to handle this by renaming to a temporary directory.
 * This temporary directory must not have existed in the origin, but must also
 * have a deterministic name for implementation of resume in the future. It
 * will be handled by detecting the first of the 3 situations and executing a
 * rename. This is overzealous, but it should break circular dependencies. The
 * alternative to a temporary rename is to unlink the file and send it again in
 * its entirety. The file size here has been made large to show that this is
 * not a good idea.
 *
 * There are the other constraints that we must handle. The case of not being
 * able to rmdir until the children are removed is simple to handle provided
 * that the children are not linked somewhere else. In that case, we need to
 * search the entire tree to see if they go somewhere else. The last of the
 * cases shown here is that we cannot unlink ./foo/a before linking ./foo. This
 * occurs whenever we have had one or more renames (or equivalently,
 * link/unlink) operations. We therefore cannot issue an unlink until we know
 * that the same file is not at a different location.
 *
 * There is also the problem of identifying what consitutes the same file. We
 * can handle that by looking at reported inode numbers and types. If the same
 * inode number with the same type is seen in both the origin and target, we
 * have seen the same file. Whether it is really the same or not does not
 * matter because the notion of same here does not mean unmodified. It only
 * means that we don't need to link/unlink/rename the file. Any file that is
 * the same (or suspected to be the same) still needs to be scrutinized for
 * differences, which occurs in stage 3.
 *
 * There is the subcase here of seeing different file types. In that case, we
 * can identify in stage 1 that the file has been unlinked and not renamed or
 * linked. Similarly, the thing we identify is a new link/creat rather than a
 * link to something pre-existing.
 *
 * There are also additional cases of handling the immutable and appendonly
 * bits on files and directories. The file case can be handled by stage 3
 * easily while the directory case would require a constraint, although it is
 * going to be handled by the receiver (Serge's suggestion).
 *
 * There are 3 main data structures involved in the 3 stages:
 *
 *	* The inode dictionary
 *	* The constraint graph
 *	* The constraint heap
 *
 * The inode dictionary is constructed in stage 1. Its primary purpose is to
 * allow link analysis by stage 2. If we did stat() operations in stage 1, we
 * could do most of the link analysis there, but it would be less efficient and
 * it complicates an already complicated process.
 *
 * The and constraint graph and constraint heap are constructed in stage 1, but
 * not finalized until stage 2. Very little of the constraint heap is finalized
 * in stage 1. The only parts that are finalized are files that are identified
 * as being "the same" and files identified as being unlinked by reuse of the
 * inode number to make something of a different type. In a producer-consumer
 * version of the code, these two cases can skip stage 2 and go straight to
 * stage 3.
 *
 * The constraint graph encapsulates all of the operations that we need to
 * instruct the other side to do, but that we cannot do until other things are
 * done while the constraint heap tells us which has zero constraints. THe keys
 * in the heap represent the number of constraints. A fundamental property that
 * our implementation must achieve is that after stage 2 has finished, the
 * constraint heap always has an item with 0 constraints or is empty.
 *
 * Stage 3 relies on the constraint heap for output generation. In a strictly
 * single threaded implementation, it runs after stage 2 and pops the heap
 * until it is empty. In producer-consumer, everything except the two mentioned
 * special cases that can skip stage 2 are given an automatic constraint count
 * of 1 before additional constraints are added. At its heart, stage 3 pops the
 * heap and does what the object popped says to do while removing any
 * constraints it placed on other things in the heap. Consequently, stage 3
 * does not actually add anything to the data structures. It just consumes them
 * while it generates the delta. It also can free memory as it does that (in
 * theory).
 *
 * This requires Linux 2.6.4 or later. It also requires a filesystem that
 * supports types in getdents64.
 */

/* Tunables (Making these bigger *might* help performance) */
const inodeSliceStartSize int = 10
const direntBufferStartSize int = 100
const sliceGrowthFactor int = 2

/* XXX: Verify this is large enough to trigger zfetch */
const getdentsBufSize int = 1048576

/* Linux-specific constants that the go runtime does not tell us. */
const oDirectory int = 0200000
const atFdcwd int = -100

/* Constants from C that the go runtime does not tell us */
const sizeofLinuxDirent64 int = 19

/* parseLocation is used when adding entries to inode map */
type parseLocation int

/*DifferenceOperations is the operations interface for consumers. All strings
 * contain paths, but no string contains the prefix to the mountpoint. Our
 * caller is expected to know that.
 *
 * ResolveDiff (origin string, target string)
 *	origin is the path on the origin mount
 *	target is the path on the target mount
 *
 *	Both data and metadata are differenced.
 *
 * Creat(path string)
 *	path in target containing regular file to be recreated.
 *
 * Link(path string, dst string)
 *	path is where we create the link
 *	dst is the link's target
 *
 * Symlink(path string)
 *	path is location in target of symlink to recreate.
 *
 * Unlink(path string)
 *	path is where the unlink command is executed
 *
 * Rmdir(path string)
 *	path is where the rmdir command is executed
 *
 * Mkdir(path string)
 *	path is location in target of directory to recreate.
 *
 * Mknod(path string)
 *	path is location in target of node to recreate.
 *
 *	This is expected to handle block devices, character devices, fifos and
 *	unix domain sockets.
 *
 * MkTmpDir(path string) - Not implemented yet
 *	path to make directory with default permissions. It is to be empty at
 *	the end and also unlinked. The only valid operation involving this
 *	directory is Rename.
 *
 * Rename(src string, dst string) - Not implemented yet
 *	Neither of these need correspond to things in the origin or target
 *	snapshots. It just needs to be sent to the other side to enable us to
 *	perform a POSIX conformant transofmration of the hierachy. This is used
 *	in conjunction with MkTmpDir.
 *
 * Operational methods:
 * ErrorChannel() chan<- error
 *       Returns the internal error reporting channel
 * ExErrorChannel() chan<- error
 *       Returns the external error reporting channel
 */
type DifferenceOperations interface {
	ResolveDiff(string, string) error
	Create(string) error
	Link(string, string) error
	Symlink(string) error
	Unlink(string) error
	Rmdir(string) error
	Mkdir(string) error
	Mknod(string) error

	// Stop checks if the differ needs to stop because of cancellation or error had happened
	Stop() bool
}

const (
	seenCommon parseLocation = 0
	seenOrigin               = 1
	seenTarget               = 2
)

type constraint int

/* The inode map is a little strange. The keys are the VFS inode numbers while
 * the values are indices into a inoderef slice. This is done to avoid
 * read-modify-write each time we want to operate on the inoderef struct and to
 * be able to grow/shrink the slice of inoderef structs without invalidating
 * references. We also gain packing from this, which ought to make life easier
 * on the garbage collector.
 *
 * If we delete an entry from the inode map, we should delete in a way that
 * avoids introducing holes, so we swap the entry being removed with the last
 * entry, remove the keys from the map and insert the key for the entry we
 * swapped into the deleted entry's place. The index of the next free space in
 * the inoderef slice then decreases by 1.
 *
 * The structs in the slice have:
 *
 *	* The inode number
 *	* The types on the origin and target
 *	* The various paths that the parser has identified.
 *
 * This is expected to be used by stage 2 to figure out:
 *
 * 	* whether an unlink was a rename
 *	* whether to move an unlink to a temporary location (which we might do
 *	overzealously) to avoid a possible circular constraints (which we
 *	probably do not want to detect)
 *      * whether a link was a create or a hard link.
 *
 * Entries in a path common to both the origin and target can have the
 * difference calculation immediately calculated by stage 3, but we need to
 * keep the references around until we have entered stage 2 just in case we
 * find a new link.
 *
 * XXX: Be very careful to handle the case that an inode number from a
 * directory entry is reused.
 */
type inoderef struct {
	commonPath []string
	unlinkPath []string
	linkPath   []string
	ino        uint64
	originType uint8
	targetType uint8
}

// State encapsulates global state for the delta algorithm
type State struct {
	originPrefix  string
	targetPrefix  string
	inodeMap      map[uint64]uint64 // Inode numbers -> Slice indices
	inodeSlice    []inoderef
	inodeCount    uint64     // Next Free Space in Slice
	constraints   vertexHeap // WIP for stage 2/3
	getdentsBuf   []byte     // Cached buffer
	diffOps       DifferenceOperations
	xattrMetadata bool
}

// dirEntSortBySame is for sorting slices of dirent
type dirEntSortBySame []dirEnt

func (a dirEntSortBySame) Len() int      { return len(a) }
func (a dirEntSortBySame) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

/* Combined comparator. This is the magic that allows our parsing algorithm to
 * categorize entries as being the same, obsolete or new without doing any
 * special set operations while handling directories separately from
 * non-directories.
 *
 * The ordering is: directories first, everything else second.
 *
 * If we are dealing with two directories, we sort by name.  If we are dealing
 * with two non-directories, we sort by inode number. If two inode numbers
 * match, we sort by name.
 *
 * Two names should never match and since there is no obvious way of returning
 * an error (aside from log.Fatalf or writing our own sort implementation), we
 * make that assumption (for now).
 */
func (a dirEntSortBySame) Less(i, j int) bool {
	if a[i].Type == syscall.DT_DIR {
		if a[j].Type == syscall.DT_DIR {
			return a[i].BaseName < a[j].BaseName
		}
		return true
	}

	if a[j].Type == syscall.DT_DIR {
		return false
	}

	if a[i].ino == a[j].ino {
		return a[i].BaseName < a[j].BaseName
	}

	return a[i].ino < a[j].ino
}

/* bytetoLinuxDirent64 converts native linux C dirent64 in byte buffer into Go
 * linuxDirent64 struct.
 */
func bytetoLinuxDirent64(buf []byte) (linuxDirent64, error) {
	var dirent linuxDirent64
	var err error
	/* Add one for empty string */
	if len(buf) < sizeofLinuxDirent64+1 {
		log.Fatalf("Kernel getdents returned invalid output")
		return dirent, err
	}
	/* XXX: This should break on a Big Endian system */
	dirent.dIno = binary.LittleEndian.Uint64(buf)
	dirent.dOff = binary.LittleEndian.Uint64(buf[8:])
	dirent.dReclen = binary.LittleEndian.Uint16(buf[16:])
	dirent.dType = buf[18]

	/* The directory entry name is variable length, but the end of it is not
	 * the end of the dirent. There can be some padding after it before the
	 * next struct starts. We find the end of string and take a subslice
	 * before giving it to string(). Otherwise, string() will convert all
	 * printable characters in the byte slice into a string.
	 */
	var i int
	for i = sizeofLinuxDirent64; i < int(dirent.dReclen); i++ {
		if buf[i] == 0 {
			break
		}
	}
	name := string(buf[sizeofLinuxDirent64:i])
	dirent.dName = string(name)
	return dirent, err
}

/* This closely follows the actual structure */
type linuxDirent64 struct {
	dIno    uint64 /* 64-bit inode number */
	dOff    uint64 /* 64-bit offset to next structure */
	dReclen uint16 /* Size of this dirent */
	dType   uint8  /* File type */
	dName   string /* Filename */
}

/* This represents a directory entry that stage 1 is processing */
type dirEnt struct {
	BaseName string // POSIX basename(1) equivalent
	DirName  string // POSIX dirname(1) equivalent
	Type     uint8
	ino      uint64
}

type dirInfo struct {
	Ents  []dirEnt
	Path  string // dirEnt.DirName
	Count int    /* Number of entries in Ents */
}

type dirEntSlice []dirEnt

/* clone exists so that we do not need to perform 2 allocations every time we
 * want to grow a slice. It also allows us to control the growth factor to be
 * something better than Go's 1.5 when appropriate.
 */
func (s dirEntSlice) clone(size int, capacity int) []dirEnt {
	t := make([]dirEnt, size, capacity)
	copy(t, s)

	return t
}

/*
 * fill initializes a dirEnt structure from a corresponding linuxDirent64 and a
 * dirname path
 */
func (d *dirEnt) fill(e *linuxDirent64, path string) {
	d.Type = e.dType
	d.DirName = path
	d.BaseName = e.dName
	d.ino = e.dIno
}

type inoderefSlice []inoderef

func (s inoderefSlice) clone(size int, capacity int) []inoderef {
	t := make([]inoderef, size, capacity)
	copy(t, s)

	return t
}

/* Easter egg: I wish Go supported generics */
type stringSlice []string

func (s stringSlice) clone(size int, capacity int) []string {
	t := make([]string, size, capacity)
	copy(t, s)

	return t
}

/* XXX: WIP structure that stage 1 is intended to build, stage 2 is intended to
 * finish and stage 3 is intended to consume. This should allow us to handle
 * rename/hardlinks properly when finished.
 */
type vertex struct {
	RefCount   int // Each constraint is a reference
	Constraint constraint
	dirEnt     *dirEnt
	ino        *inoderef
}

type vertexHeap []vertex

func (h vertexHeap) Len() int           { return len(h) }
func (h vertexHeap) Less(i, j int) bool { return h[i].RefCount < h[j].RefCount }
func (h vertexHeap) Swap(i, j int)      { h[i].RefCount, h[j].RefCount = h[j].RefCount, h[i].RefCount }

func (h *vertexHeap) Push(x interface{}) {
	item := x.(vertex)
	*h = append(*h, item)
}

func (h *vertexHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

/* XXX: Probably inefficient */
func (s stringSlice) add(v string) []string {
	size := cap(s)
	t := s.clone(size+1, size+1)
	t[cap(s)] = v
	return t
}

type traverseFunc func(*linuxDirent64, int, interface{}) error

/* traverseDirImpl is a iteration function. It calls converts each directory
 * entry from a C structure into a Go structure and invokes the callback
 * function cb on each one. The callback gets the directory entry, the file
 * descriptor and a private pointer. The file descriptor is provided to allow
 * for recursion and also stat (if necessary). The callback should never close
 * the file descriptor.
 *
 * It was based on the Linux man page
 * example. The byte buffer buf is used to receive the directory entries from
 * the kernel and is intended to allow for caching.
 */
func traverseDirImpl(fd int, buf []byte, cb traverseFunc, arg interface{}) error {
	var d linuxDirent64
	for {
		nread, err := syscall.Getdents(fd, buf)
		if err != nil {
			return err
		}

		/* This indicates we have reached the end of directory */
		if nread == 0 {
			break
		}

		bpos := 0
		for bpos < nread {
			d, err = bytetoLinuxDirent64(buf[bpos:])
			if err != nil {
				return err
			}

			err = cb(&d, fd, arg)
			if err != nil {
				return err
			}

			bpos += int(d.dReclen)
		}

		/* getdents64 entries have a maximum length of math.MaxUint16.
		 * If there is at least that much space left in the buffer, we
		 * can assume that we have reached the end of the directory. If
		 * we do not have that much space left, we could still reach
		 * the end of the directory, but we need to do another system
		 * call to learn that. In that case, we break on nread == 0. We
		 * could delete this conditional branch and things would still
		 * work. It would just be slower.
		 */
		if len(buf)-nread > math.MaxUint16 {
			break
		}
	}
	return nil
}

func (g *State) traverseDir(fd int, cb traverseFunc, arg interface{}) error {
	return traverseDirImpl(fd, g.getdentsBuf, cb, arg)
}

/* insertDirIntoSlice is a callback function for traverseDir. It is passed a
 * reference to a dirInfo struct and builds up a slice containing a list of
 * entries in the directory.
 *
 * XXX: We rely on the getdents64 system call to obtain type information so
 * that we can defer calling stat until stage 3. This does not work on
 * filesystems that lack support for reporting type information in getdent64,
 * which might include XFS. When d.dType == DT_UNKNOWN, we should call stat as
 * a fallback. We probably should consider cache the information to avoid
 * needing to call stat later.
 */
func insertDirIntoSlice(d *linuxDirent64, fd int, arg interface{}) error {
	info := arg.(*dirInfo)

	if d.dName[0] == '.' && (len(d.dName) == 1 ||
		(len(d.dName) == 2 && d.dName[1] == '.')) {
		return nil
	}

	// If the slice is too small, grow it to size
	if cap(info.Ents) == info.Count {
		newsize := (cap(info.Ents) + 1) * sliceGrowthFactor
		info.Ents = dirEntSlice(info.Ents).clone(newsize, newsize)
	}
	info.Ents[info.Count].fill(d, info.Path)
	info.Count++

	return nil
}

/* scanDir returns a dirInfo struct containing a description of the directory */
func (g *State) scanDir(dirfd int, path string) (dirInfo, error) {
	var d dirInfo
	var err error

	/* Size initial slice by estimating with directory link count */
	d.Ents = make([]dirEnt, direntBufferStartSize)
	d.Path = path

	if err = g.traverseDir(dirfd, insertDirIntoSlice, &d); err != nil {
		return d, err
	}

	/* Resize slice so that garbage collector can free unneeded memory */
	d.Ents = dirEntSlice(d.Ents).clone(d.Count, d.Count)

	sort.Sort(dirEntSortBySame(d.Ents))

	return d, err
}

/* Stub */
func (g *State) addVertex(t constraint, refcount int, d *dirEnt) {
	v := vertex{Constraint: t, RefCount: refcount}
	heap.Push(&g.constraints, v)
}

/*
 * This is a stub that provides a quick and dirty way of comparing two files on
 * the origin and target for equality. It wrongly assumes that the modification
 * time determines whether two files are the same. It does not yet handle file
 * attributes.
 * Extended attributes/ACLs are handled by the callee.
 *
 * XXX: Do more rigorous comparison
 */
func (g *State) isDiff(origin string, target string) (bool, error) {
	originPath := filepath.Join(g.originPrefix, origin)
	targetPath := filepath.Join(g.targetPrefix, target)

	originStat, errOStat := os.Lstat(originPath)
	if errOStat != nil {
		return true, errOStat
	}

	targetStat, errTStat := os.Lstat(targetPath)
	if errTStat != nil {
		return true, errTStat
	}

	if originStat.Size() != targetStat.Size() {
		return true, nil
	}

	if originStat.Mode() != targetStat.Mode() {
		return true, nil
	}

	if originStat.Mode() != targetStat.Mode() {
		return true, nil
	}

	if originStat.ModTime() != targetStat.ModTime() {
		return true, nil
	}

	//if xattrs are involved, we declare files different
	//and let the lower-level code deal with diffing them.
	xattrOrigin, errXatrListO := hasXattrs(originPath)
	if errXatrListO != nil {
		return true, errXatrListO
	}

	xattrTarget, errXatrListT := hasXattrs(targetPath)
	if errXatrListT != nil {
		return true, errXatrListT
	}

	if xattrOrigin || xattrTarget {
		return true, nil
	}

	return false, nil
}

func hasXattrs(filepath string) (bool, error) {
	xattrs, err := xattr.Listxattr(filepath)
	if err != nil {
		return true, err
	}
	return len(xattrs) > 0, nil
}

func (g *State) addToInodeMap(d *dirEnt, l parseLocation) error {
	if g.diffOps.Stop() {
		return nil
	}

	if uint64(cap(g.inodeSlice)) == g.inodeCount {
		newsize := (cap(g.inodeSlice) + 1) * sliceGrowthFactor
		g.inodeSlice = inoderefSlice(g.inodeSlice).clone(newsize, newsize)
	}

	index, exists := g.inodeMap[d.ino]
	if !exists {
		index = g.inodeCount
		g.inodeMap[d.ino] = index
		g.inodeCount++
	}

	iref := &g.inodeSlice[index]
	iref.ino = d.ino
	path := filepath.Join(d.DirName, d.BaseName)

	/* We print here to show where we can do diff, link, unlink
	 * operations if using only the parser. However, this does not
	 * preserve hardlinks or handle rename properly.
	 */
	var err error
	switch l {
	case seenCommon:
		diff, _ := g.isDiff(path, path)
		if !exists && diff {
			err = g.diffOps.ResolveDiff(path, path)
			if err != nil {
				break
			}
		}
		iref.originType = d.Type
		iref.targetType = d.Type

		iref.commonPath = stringSlice(iref.commonPath).add(path)
		break
	case seenOrigin:
		if d.Type == syscall.DT_DIR {
			err = g.diffOps.Rmdir(path)
			if err != nil {
				break
			}
		} else {
			err = g.diffOps.Unlink(path)
			if err != nil {
				break
			}
		}
		iref.originType = d.Type
		iref.unlinkPath = stringSlice(iref.unlinkPath).add(path)
		break
	case seenTarget:
		if d.Type == syscall.DT_DIR {
			err = g.diffOps.Mkdir(path)
			if err != nil {
				break
			}
		} else {
			var link *string
			if len(iref.commonPath) > 0 {
				link = &iref.commonPath[0]
			} else if len(iref.linkPath) > 0 {
				link = &iref.linkPath[0]
			}

			if link != nil {
				err = g.diffOps.Link(path, *link)
				if err != nil {
					break
				}
			} else {
				if g.xattrMetadata && d.Type == syscall.DT_REG {
					xattr, err := xattr.Getxattr(path, record.XattrPrefix+"devmode")
					if err == nil {
						devmode, err := strconv.ParseInt(string(xattr), 0, 32)
						if err != nil {
							log.Println("Error: failed to parse devmode ", string(xattr))
							/*XXX: Add error handling */
						} else {
							if (devmode & syscall.S_IFBLK) != 0 {
								d.Type = syscall.DT_BLK
							} else if (devmode & syscall.S_IFCHR) != 0 {
								d.Type = syscall.DT_CHR
							} else if (devmode & syscall.S_IFIFO) != 0 {
								d.Type = syscall.DT_FIFO
							} else if (devmode & syscall.S_IFSOCK) != 0 {
								d.Type = syscall.DT_SOCK
							}
						}
					}
				}
				switch d.Type {
				case syscall.DT_LNK:
					err = g.diffOps.Symlink(path)
					break
				case syscall.DT_REG:
					err = g.diffOps.Create(path)
					break
				case syscall.DT_BLK, syscall.DT_CHR, syscall.DT_FIFO, syscall.DT_SOCK:
					err = g.diffOps.Mknod(path)
					break
				default:
					log.Println("Error: unknown type ", d.Type)
					/*XXX: Add error handling */
					break
				}
			}
		}
		iref.targetType = d.Type
		iref.linkPath = stringSlice(iref.linkPath).add(path)
		break
	}

	return err
}

/* processUniqueSubtree is a helper function for processCommonSubtree. It handles
 * subtrees that exist on only the origin or the target snapshot. It
 * recursively traverses the subtree and adds entries to the inode map. See the
 * description of processCommonSubtree for more information.
 */
func (g *State) processUniqueSubtree(fd int, d *dirEnt, l parseLocation) error {
	var err error
	/* Link on directories is pre-order */
	if l == seenTarget {
		err = g.addToInodeMap(d, l)
		if err != nil {
			return err
		}
	}

	if d.Type == syscall.DT_DIR {
		var info dirInfo
		newfd, err := syscall.Openat(fd, d.BaseName,
			syscall.O_RDONLY|oDirectory, 0)

		if err == nil {
			path := filepath.Join(d.DirName, d.BaseName)
			info, err = g.scanDir(newfd, path)
			if err == nil {
				for _, e := range info.Ents {
					g.processUniqueSubtree(newfd, &e, l)
				}
			}

		}

		if err == nil {
			err = syscall.Close(newfd)
		} else {
			_ = syscall.Close(newfd)
		}

		if err != nil {
			return err
		}
	}
	/* Unlink on directories is post-order */
	if l == seenOrigin {
		err = g.addToInodeMap(d, l)
	}
	return err
}

/* processCommonSubtree is the heart of the parsing algorithm. It runs under
 * the assumption that the path in both trees is common. It is initially called
 * on the root path. The State struct provides the actual prefixes for
 * filesystem accesses, among other things needed by its helpers. The fd
 * []slice argument should have exactly 2 elements. fd[0] is an open file
 * descriptor for the origin snapshot. fd[1] is an open file descriptor for the
 * target snapshot. They determine the initial starting point for the
 * algorithm. The path string argument shows where in the common tree the
 * algorithm is and is not needed for the traversal to work. It exists solely
 * for population of directory entry structures so that later stages know what
 * to emit.
 *
 * The parser reads the entries of the current directory into slices with a
 * special ordering where directories precede non-directories. Directories
 * themselves are sorted by name while non-directories are sorted by inode
 * number and by name when two entries have the same inode number. The parser
 * then performs an O(N) operation on the directory entries to identify those
 * that are:
 *
 *	* unique in the origin (unlinked)
 *	* common to both
 *	* unique in the target (linked).
 *
 * It resembles a stage in merge sort. It calls itself on common
 * sub-directories and a processUniqueSubtree helper on unique directories.  The
 * algorithm then performs the same merge-like operation on non-directories.
 *
 * On directory entries for directories unique to either the origin or target,
 * the processUniqueSubtree helper will call addToInodeMap on each inode. It is
 * called pre-order on entries unique to the target and post-order on entries
 * unique to the origin.  processCommonSubtree handles directory entries for
 * directories common to both by calling itself. It will call addToInodeMap in
 * pre-order. The ordering on unique entries matters for later stages. The
 * ordering on common entries was chosen arbitrarily. Either order should work
 * on common entries, but it must be consistent (forever) if we are to support
 * resume without doing a format version bump.
 *
 * On directory entries for non-directories, processCommonSubtree will call
 * addToInodeMap directly.
 *
 * XXX: We could amortize memory allocation overhead by using two slices, one
 * for the origin directory entries and target directory entries each, such
 * that each stack frame would operate on a segment of it.
 */
func (g *State) processCommonSubtree(fd []int, path string) error {
	var (
		origin, target dirInfo
		err            error
	)

	if origin, err = g.scanDir(fd[0], path); err != nil {
		return err
	}

	if target, err = g.scanDir(fd[1], path); err != nil {
		return err
	}

	/* Process origin and target until one is out of directories */
	var i, j int = 0, 0
	for i < len(origin.Ents) && j < len(target.Ents) {
		d := &origin.Ents[i]
		e := &target.Ents[j]

		if origin.Ents[i].Type != syscall.DT_DIR || target.Ents[j].Type != syscall.DT_DIR {
			break
		}

		/* XXX: Is strings.Compare unicode safe? */
		switch strings.Compare(d.BaseName, e.BaseName) {
		case 0:
			err = g.addToInodeMap(e, seenCommon)
			if err != nil {
				break
			}

			newfd := []int{-1, -1}
			var k int

			for k < 2 {
				newfd[k], err = syscall.Openat(fd[k], e.BaseName,
					syscall.O_RDONLY|oDirectory, 0)

				k++
				if err != nil {
					break
				}
			}

			if err == nil {
				path := filepath.Join(e.DirName, e.BaseName)
				err = g.processCommonSubtree(newfd, path)
			}

			for k--; k >= 0; k-- {
				/* Failing here should imply a bug in the code
				 * closed a file descriptor early. This really
				 * should be an assertion, but for now, just
				 * pass the error unless we already failed, in
				 * which case, we avoid clobbering the error.
				 */
				if err != nil {
					_ = syscall.Close(newfd[k])
				} else {
					err = syscall.Close(newfd[k])
				}
			}

			i++
			j++
			break
		case -1:
			err = g.processUniqueSubtree(fd[0], d, seenOrigin)
			i++
			break
		case 1:
			err = g.processUniqueSubtree(fd[1], e, seenTarget)
			j++
			break
		}

		if err != nil {
			return err
		}
	}

	/* Process leftover directories in origin, if any exist */
	for i < len(origin.Ents) && origin.Ents[i].Type == syscall.DT_DIR {
		d := &origin.Ents[i]
		if err = g.processUniqueSubtree(fd[0], d, seenOrigin); err != nil {
			return err
		}
		i++
	}

	/* Process leftover directories in target, if any exist */
	for j < len(target.Ents) && target.Ents[j].Type == syscall.DT_DIR {
		e := &target.Ents[j]
		if err = g.processUniqueSubtree(fd[1], e, seenTarget); err != nil {
			return err
		}
		j++
	}

	/* Process non-directories in origin and target until either is empty */
	for i < len(origin.Ents) && j < len(target.Ents) {
		d := &origin.Ents[i]
		e := &target.Ents[j]
		if d.ino != e.ino {
			if d.ino < e.ino {
				err = g.addToInodeMap(d, seenOrigin)
				if err != nil {
					return err
				}
				i++
			} else {
				/*
				 * If origin.Ents and target.Ents have a name
				 * collision between different inodes, we must
				 * avoid the situation where they are processed
				 * in the wrong order by processing the one in
				 * origin.Ents first. This would happen if the
				 * origin inode number is greater than the
				 * target inode number. When this happens, we
				 * would emit create and then unlink without
				 * this kludge. With this kludge, we find the
				 * collision, swap the entry with the next one
				 * in origin.Ents and process it first to avoid
				 * this condition.
				 *
				 * XXX: We should optimize this by identifying
				 * collisions in advance and processing them
				 * once, rather than checking each time a
				 * collision might be possible. This is not a
				 * commonly executed codepath and this code
				 * will likely change once hardlinks are
				 * handled properly, so this is left as an
				 * exercise for then.
				 */
				for index, entry := range origin.Ents[i:] {
					if strings.Compare(entry.BaseName, e.BaseName) == 0 {
						origin.Ents[i], origin.Ents[index] = origin.Ents[index], origin.Ents[i]
						err = g.addToInodeMap(&entry, seenOrigin)
						if err != nil {
							return err
						}
						i++
						break
					}
				}
				err = g.addToInodeMap(e, seenTarget)
				if err != nil {
					return err
				}
				j++
			}
		} else if d.Type != e.Type {
			err = g.addToInodeMap(d, seenOrigin)
			if err != nil {
				return err
			}
			err = g.addToInodeMap(e, seenTarget)
			if err != nil {
				return err
			}
			i++
			j++
		} else {
			switch strings.Compare(d.BaseName, e.BaseName) {
			case 0:
				err = g.addToInodeMap(d, seenCommon)
				i++
				j++
				break
			case -1:
				err = g.addToInodeMap(d, seenOrigin)
				i++
				break
			case 1:
				err = g.addToInodeMap(e, seenTarget)
				j++
				break
			}
			if err != nil {
				return err
			}
		}
	}

	/* Process leftover non-directories in origin, if any exist */
	for i < len(origin.Ents) {
		d := &origin.Ents[i]
		err = g.addToInodeMap(d, seenOrigin)
		if err != nil {
			return err
		}
		i++
	}

	/* Process leftover non-directories in target, if any exist */
	for j < len(target.Ents) {
		e := &target.Ents[j]
		err = g.addToInodeMap(e, seenTarget)
		if err != nil {
			return err
		}
		j++
	}

	return nil
}

// DoParsing does a depth first search traversal of the filesystem using the
// openat and getdents system calls to generate a parse graph. Each directory
// has 1 open() operation and 1 or more getdents operations on each snapshot.
// The number of getdents operations can be reduced on snapshots with large
// directories by increasing getdentsBufSize. Absolute paths are used for the
// root directories while relative paths are used for all other directories.
// This speeds up path lookups inside the kernel.
func (g *State) DoParsing() error {
	fd := []int{-1, -1}
	path := []string{g.originPrefix, g.targetPrefix}
	var err error
	var i int

	/* Verify prefixes are directories via oDirectory */
	for i < 2 {
		fd[i], err = syscall.Openat(atFdcwd, path[i],
			syscall.O_RDONLY|oDirectory, 0)

		i++
		if err != nil {
			break
		}
	}

	if err == nil {
		err = g.processCommonSubtree(fd, "")
	}

	for i--; i >= 0; i-- {
		/*
		 * Failing here should imply a bug in the code closed a file
		 * descriptor early. This really should be an assertion, but
		 * for now, just pass the error unless we already failed, in
		 * which case, we avoid clobbering the error.
		 */
		if err != nil {
			_ = syscall.Close(fd[i])
		} else {
			err = syscall.Close(fd[i])
		}
	}

	return err
}

// Alloc is our State constructor
func Alloc(origin string, target string, dops DifferenceOperations, xattrMetadata bool) *State {
	g := new(State)
	g.inodeMap = make(map[uint64]uint64)
	g.inodeSlice = make([]inoderef, inodeSliceStartSize)
	g.inodeCount = 0
	g.getdentsBuf = make([]byte, getdentsBufSize)
	g.originPrefix = origin
	g.targetPrefix = target
	g.diffOps = dops
	g.xattrMetadata = xattrMetadata

	heap.Init(&g.constraints)
	return g
}
