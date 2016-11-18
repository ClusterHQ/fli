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

// Package record defines interfaces for syscall records, executor and encoder/decoder.
// It also has implementations of all syscall records, gob encoder/decoder, and a common executor and the stdout
// executor.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"os/user"
	"path"
	"reflect"
	"strconv"
	"syscall"
	"time"

	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/pkg/xattr"
)

// IMPORTANT: Any new record type should be register with gob
// encoding/decoding (see gob_encdec.go)

const (
	// DefaultCreateMode - create file with these default permissions
	DefaultCreateMode os.FileMode = 0777

	// XattrPrefix is a prefix that is used for safely storing file attributes and extended attributes
	// as extended attributes in the user namespace.
	XattrPrefix string = "user.com.clusterhq.attributes."

	// User and group names that are used when an ID can not be mapped to a name
	// or a name is unknown on a system.
	fallbackUser   string = "nobody"
	fallbackGroup1 string = "nogroup"
	fallbackGroup2 string = "nobody"
)

type (
	// Hdr has common fields for all records. Note: Export Hdr and Chksum because of GOB.
	Hdr struct {
		Chksum []byte
	}

	// Mkdir represents a record for mkdir
	// Note: All record type can be unexported, but GOB needs to register them, so they are exported.
	// Note: All fields are exported because gob only encodes exported fields
	Mkdir struct {
		Hdr
		Path string
		Mode os.FileMode
	}

	// Pwrite represents a record for pwrite
	Pwrite struct {
		Hdr
		Path   string
		Data   []byte
		Offset uint64
		Mode   os.FileMode // always zero, kept for compatibility
	}

	// Hardlink represents a record for link
	Hardlink struct {
		Hdr
		OldName string
		NewName string
	}

	// Symlink represents a record for creation of a symlink
	Symlink struct {
		Hdr
		OldName string
		NewName string
	}

	// Truncate represents a record for truncation of a file
	Truncate struct {
		Hdr
		Path string
		Size int64
	}

	// Chown represents a record for doing a chown on a file
	// TODO: might get absorbed into SETATTR record later
	Chown struct {
		Hdr
		Path string
		UID  string // XXX stored as a name, not ID
		GID  string // XXX stored as a name, not ID
	}

	// Create represents a record for create/open/mkfile
	Create struct {
		Hdr
		Path string
		Mode os.FileMode
	}

	// Remove represents a record for removal of file or directory
	Remove struct {
		Hdr
		Path string
	}

	// Setxattr represents a record for setting extended attribute
	Setxattr struct {
		Hdr
		Path  string
		Xattr string
		Data  []byte
	}

	// Rmxattr represents a record for removing extended attribute
	Rmxattr struct {
		Hdr
		Path  string
		Xattr string
	}

	// Mknod represent a record for mknod
	Mknod struct {
		Hdr
		Path string
		Mode uint32
		Dev  int
	}

	// Rename represents a record for rename
	Rename struct {
		Hdr
		OldPath string
		NewPath string
	}

	// Chmod represents a record for chmod
	Chmod struct {
		Hdr
		Path string
		Mode os.FileMode
	}

	// Setmtime represents a record for modify time
	Setmtime struct {
		Hdr
		Path  string
		MTime int64
	}

	// EOT represents a special record which does nothing to the file system but indicates the end of a transfer
	EOT struct {
		Hdr
		Name string
	}

	// ExecType defines the execution type for each record
	ExecType int

	// Type defines the record type for each record
	Type int
)

const (
	// AsyncExec is executed asynchronously(e.x. pwrie)
	AsyncExec ExecType = iota
	// SyncExec is executed synchronously(e.x. create dir)
	SyncExec
	// DelayExec is executed at the end of all asynchronous records are executed(e.x. setmtime)
	DelayExec

	// TypeMkdir defines the type ID
	TypeMkdir Type = iota
	// TypePwrite defines the type ID
	TypePwrite
	// TypeHardlink defines the type ID
	TypeHardlink
	// TypeSymlink defines the type ID
	TypeSymlink
	// TypeTruncate defines the type ID
	TypeTruncate
	// TypeChown defines the type ID
	TypeChown
	// TypeCreate defines the type ID
	TypeCreate
	// TypeRemove defines the type ID
	TypeRemove
	// TypeSetxattr defines the type ID
	TypeSetxattr
	// TypeRmxattr defines the type ID
	TypeRmxattr
	// TypeRename defines the type ID
	TypeRename
	// TypeMknod defines the type ID
	TypeMknod
	// TypeChmod defines the type ID
	TypeChmod
	// TypeSetmtime defines the type ID
	TypeSetmtime

	// TypeEOT defines the type ID
	TypeEOT
)

// Record defines the function signature for all the common methods implemented for each record type
type Record interface {
	Exec(root string) error
	String() string

	// Key returns a string that can be used to group records that belong to the same object(for example, file path)
	Key() string

	// ExeType returns the execution type of a record
	ExecType() ExecType

	// Type returns the type of a record
	Type() Type

	// ToBinary is the binary encoder which writes the record in binary format
	ToBinary(io.Writer) error

	// FromBinary reads the record from an encode source and populates the record with the data read from the source
	FromBinary(io.Reader) error

	// Chksum returns the checksum of a record.
	// Note: It doesn't set the record's checksum field, so it can be used to recalculate the checksum for
	// verification.
	Chksum(dlhash.Factory) ([]byte, error)

	// SetChksum sets the record's checksum
	SetChksum([]byte)

	// GetChksum returns the record's checksum
	GetChksum() []byte
}

// SafeRecord is a subtype of Record that has a method for
// faking security-sensitive operations.
type SafeRecord interface {
	Record
	SafeExec(root string) error
}

// Note: Make sure -
// 1. All syscall records implement interface Record or SafeRecord
// 2. Add to encoder/decoder(GOB, binary)
var (
	_ SafeRecord = &Mkdir{}
	_ Record     = &Pwrite{}
	_ Record     = &Hardlink{}
	_ Record     = &Symlink{}
	_ Record     = &Truncate{}
	_ SafeRecord = &Chown{}
	_ SafeRecord = &Create{}
	_ Record     = &Remove{}
	_ Record     = &Rename{}
	_ SafeRecord = &Mknod{}
	_ SafeRecord = &Setxattr{}
	_ SafeRecord = &Rmxattr{}
	_ SafeRecord = &Chmod{}
	_ Record     = &Setmtime{}
	_ Record     = &EOT{}
)

// SetChksum sets the record's checksum
func (h *Hdr) SetChksum(chksum []byte) {
	// Note: TO get around the different encoder/decoder returns different empty []byte
	if len(chksum) == 0 {
		h.Chksum = nil
	} else {
		h.Chksum = chksum
	}
}

// GetChksum returns the record's checksum
func (h *Hdr) GetChksum() []byte {
	// Note: TO get around the different encoder/decoder returns different empty []byte
	if len(h.Chksum) == 0 {
		h.Chksum = nil
	}
	return h.Chksum
}

// MKDIR-SPECIFIC INTERFACE IMPLEMENTATION//////////////////////////////////
// The functions below describe common mkdir() related functionality.
// If there are special mkdir() actions required for a certain distribution,
// the functions below will need to be overriden by the code that is
// exectuing for that distribution.

// NewMkdir returns an record object of type mkdir from the parameters
func NewMkdir(path string, mode os.FileMode) Record {
	return &Mkdir{
		Path: path,
		Mode: mode,
	}
}

// Exec is the implementation of Record interface
func (rec *Mkdir) Exec(root string) error {
	return os.Mkdir(path.Join(root, rec.Path), rec.Mode)
}

// SafeExec is implementation of FakeRecord interface
func (rec *Mkdir) SafeExec(root string) error {
	err := os.Mkdir(path.Join(root, rec.Path), DefaultCreateMode)
	if err != nil {
		return err
	}
	return xattr.Setxattr(path.Join(root, rec.Path), XattrPrefix+"mode", []byte(strconv.FormatUint(uint64(rec.Mode), 8)))
}

// String is the implementation of Record interface
func (rec *Mkdir) String() string {
	return fmt.Sprintf("%T: path = %s, mode = %d", rec, rec.Path, rec.Mode)
}

// Key is implementation of Record interface
func (rec *Mkdir) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Mkdir) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Mkdir) Type() Type {
	return TypeMkdir
}

// ToBinary is implementation of Record interface
func (rec *Mkdir) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Mode))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Mkdir) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.Mode = os.FileMode(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Mkdir) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.Mode))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// PWRITE SPECIFIC  ////////////////////////////////////////////////////////
// The functions below describe common pwrite() related functionality.
// If there are special pwrite() actions required for a certain
// distribution, the functions below will need to be overriden by the
// code that is exectuing for that distribution.

// NewPwrite returns an record object of type pwrite from the parameters
func NewPwrite(path string, data []byte, offset uint64) Record {
	return &Pwrite{
		Path:   path,
		Data:   data,
		Offset: offset,
		Mode:   0,
	}
}

// Exec is implementation of Record interface
func (rec *Pwrite) Exec(root string) error {
	// Open the file for writing; Note: mode is only needed with O_CREATE
	fp, err := os.OpenFile(path.Join(root, rec.Path), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.WriteAt(rec.Data, int64(rec.Offset))
	if err != nil {
		return err
	}
	return nil
}

func (rec *Pwrite) String() string {
	return fmt.Sprintf("%T: file = %s, len = %d, offset = %d",
		rec, rec.Path, len(rec.Data), rec.Offset)
}

// Key is implementation of Record interface
func (rec *Pwrite) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Pwrite) ExecType() ExecType {
	return AsyncExec
}

// Type is implementation of Record interface
func (rec *Pwrite) Type() Type {
	return TypePwrite
}

// ToBinary is implementation of Record interface
func (rec *Pwrite) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Data)
	if err != nil {
		return err
	}

	err = writeUint64(target, rec.Offset)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Mode))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Pwrite) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	data, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Data = data

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.Offset = n

	n, err = readUint64(src)
	if err != nil {
		return err
	}
	rec.Mode = os.FileMode(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Pwrite) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashBytes(h, rec.Data)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, rec.Offset)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// CREATE-SPECIFIC  ////////////////////////////////////////////////////////////////
// The functions below describe common create/mkfile-related functionality.
// If there are special create actions required for a certain distribution,
// the functions below will need to be overriden by the code that is exectuing
// for that distribution.

// NewCreate returns an record object of type create from the parameters
func NewCreate(path string, mode os.FileMode) Record {
	return &Create{
		Path: path,
		Mode: mode,
	}
}

// Exec is implementation of Record interface
// Creates a new file. Corresponds to 'touch' or 'mkfile', etc.
func (rec *Create) Exec(root string) error {
	fp, err := os.OpenFile(path.Join(root, rec.Path), os.O_WRONLY|os.O_CREATE|os.O_EXCL, rec.Mode)
	if err != nil {
		return err
	}
	fp.Close()
	return nil
}

// SafeExec is implementation of Record interface
// Creates a new file. Corresponds to 'touch' or 'mkfile', etc.
func (rec *Create) SafeExec(root string) error {
	// this will create the file with the default mode
	fp, err := os.Create(path.Join(root, rec.Path))
	if err != nil {
		return err
	}
	defer fp.Close()
	return xattr.Setxattr(path.Join(root, rec.Path), XattrPrefix+"mode", []byte(strconv.FormatUint(uint64(rec.Mode), 8)))
}

func (rec *Create) String() string {
	return fmt.Sprintf("%T: file = %s, mode = %d", rec, rec.Path, rec.Mode)
}

// Key is implementation of Record interface
func (rec *Create) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Create) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Create) Type() Type {
	return TypeCreate
}

// ToBinary is implementation of Record interface
func (rec *Create) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Mode))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Create) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.Mode = os.FileMode(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Create) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.Mode))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// REMOVE-SPECIFIC INTERFACE IMPLEMENTATION//////////////////////////////////////////
// The functions below describe common remove() related functionality.
// If there are special remove() actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewRemove returns an record object of type remove from the parameters
func NewRemove(path string) Record {
	return &Remove{
		Path: path,
	}
}

// Exec is implementation of Record interface
func (rec *Remove) Exec(root string) error {
	return os.Remove(path.Join(root, rec.Path))
}

func (rec *Remove) String() string {
	return fmt.Sprintf("%T: filename = %s", rec, rec.Path)
}

// Key is implementation of Record interface
func (rec *Remove) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Remove) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Remove) Type() Type {
	return TypeRemove
}

// ToBinary is implementation of Record interface
func (rec *Remove) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Remove) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	return nil
}

// Chksum is implementation of Record interface
func (rec *Remove) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// LINK-SPECIFIC INTERFACE IMPLEMENTATION///////////////////////////////////////////
// The functions below describe common link() related functionality.
// If there are special link() actions required for a certain distribution,
// the functions below will need to be overriden by the code that is exectuing
// for that distribution.

// NewHardlink returns an record object of type hardlink from the parameters.
func NewHardlink(oldname string, newname string) Record {
	return &Hardlink{
		OldName: oldname,
		NewName: newname,
	}
}

// Exec is implementation of Record interface
func (rec *Hardlink) Exec(root string) error {
	return os.Link(path.Join(root, rec.OldName), path.Join(root, rec.NewName))
}

func (rec *Hardlink) String() string {
	return fmt.Sprintf("%T: oldname = %s, newname = %s", rec, rec.OldName, rec.NewName)
}

// Key is implementation of Record interface
func (rec *Hardlink) Key() string {
	return rec.OldName
}

// ExecType is implementation of Record interface
func (rec *Hardlink) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Hardlink) Type() Type {
	return TypeHardlink
}

// ToBinary is implementation of Record interface
func (rec *Hardlink) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.OldName)
	if err != nil {
		return err
	}

	err = writeString(target, rec.NewName)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Hardlink) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.OldName = path

	path, err = readString(src)
	if err != nil {
		return err
	}
	rec.NewName = path

	return nil
}

// Chksum is implementation of Record interface
func (rec *Hardlink) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.OldName)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.NewName)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// SYMLINK-SPECIFIC INTERFACE IMPLEMENTATION////////////////////////////////////////
// The functions below describe common symlink-related functionality.
// If there are special symlink actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewSymlink returns an record object of type symlink from the parameters.
func NewSymlink(oldname string, newname string) Record {
	return &Symlink{
		OldName: oldname,
		NewName: newname,
	}
}

// Exec is implementation of Record interface
// Note: code executing this needs to have appropriate permissions (s.a. be run as superuser)
func (rec *Symlink) Exec(root string) error {
	// NOTE: The target path to the symlink always be under the root. The differ should take care of this and the
	//       target path will be relative to the symlink. Hence we can create the symlink's to target link without
	//       altering the path.
	return os.Symlink(rec.OldName, path.Join(root, rec.NewName))
}

func (rec *Symlink) String() string {
	return fmt.Sprintf("%T: oldname = %s, newname = %s", rec, rec.OldName, rec.NewName)
}

// Key is implementation of Record interface
func (rec *Symlink) Key() string {
	return rec.OldName
}

// ExecType is implementation of Record interface
func (rec *Symlink) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Symlink) Type() Type {
	return TypeSymlink
}

// ToBinary is implementation of Record interface
func (rec *Symlink) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.OldName)
	if err != nil {
		return err
	}

	err = writeString(target, rec.NewName)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Symlink) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.OldName = path

	path, err = readString(src)
	if err != nil {
		return err
	}
	rec.NewName = path

	return nil
}

// Chksum is implementation of Record interface
func (rec *Symlink) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.OldName)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.NewName)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// TRUNCATE-SPECIFIC INTERFACE IMPLEMENTATION///////////////////////////////////////
// The functions below describe common truncate-related functionality.
// If there are special truncate actions required for a certain distribution,
// the functions below will need to be overridden by the code that is executing
// for that distribution.

// NewTruncate returns an record object of type truncate from the parameters.
func NewTruncate(filepath string, size int64) Record {
	return &Truncate{
		Path: filepath,
		Size: size,
	}
}

// Exec is implementation of Record interface
func (rec *Truncate) Exec(root string) error {
	return os.Truncate(path.Join(root, rec.Path), rec.Size)
}

func (rec *Truncate) String() string {
	return fmt.Sprintf("%T: filename = %s, size = %d", rec, rec.Path, rec.Size)
}

// Key is implementation of Record interface
func (rec *Truncate) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Truncate) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Truncate) Type() Type {
	return TypeTruncate
}

// ToBinary is implementation of Record interface
func (rec *Truncate) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Size))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Truncate) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.Size = int64(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Truncate) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.Size))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// CHOWN-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common chown-related functionality.
// If there are special chown actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewChown returns an record object of type chown from the parameters.
func NewChown(filepath string, uid int, gid int) Record {
	uidStr := fallbackUser
	userObj, err := user.LookupId(uidStr)
	if err == nil {
		uidStr = userObj.Username
	}

	gidStr := fallbackGroup1
	groupObj, err := user.LookupGroupId(gidStr)
	if err == nil {
		gidStr = groupObj.Name
	}

	return &Chown{
		Path: filepath,
		UID:  uidStr,
		GID:  gidStr,
	}
}

// NewChownByNames returns an record object of type chown from the parameters.
func NewChownByNames(filepath string, uid string, gid string) Record {
	return &Chown{
		Path: filepath,
		UID:  uid,
		GID:  gid,
	}
}

// Exec is implementation of Record interface
func (rec *Chown) Exec(root string) error {
	userObj, err := user.Lookup(rec.UID)
	if err != nil {
		userObj, err = user.Lookup(fallbackUser)
		if err != nil {
			return fmt.Errorf(
				"user %s and fallback user %s cannot be found: %v",
				rec.UID,
				fallbackUser,
				err,
			)
		}
	}
	uid, err := strconv.Atoi(userObj.Uid)
	if err != nil {
		return fmt.Errorf(
			"failed to convert uid %s to number: %v",
			userObj.Uid,
			err,
		)
	}

	groupObj, err := user.LookupGroup(rec.GID)
	if err != nil {
		groupObj, err = user.LookupGroup(fallbackGroup1)
		if err != nil {
			groupObj, err = user.LookupGroup(fallbackGroup2)
			if err != nil {
				return fmt.Errorf(
					"group %s and fallback groups %s, %s cannot be found: %v",
					rec.GID,
					fallbackGroup1,
					fallbackGroup2,
					err,
				)
			}
		}
	}
	gid, err := strconv.Atoi(groupObj.Gid)
	if err != nil {
		return fmt.Errorf(
			"failed to convert gid %s to number: %v",
			groupObj.Gid,
			err,
		)
	}
	return os.Lchown(path.Join(root, rec.Path), uid, gid)
}

// SafeExec is implementation of FakeRecord interface
func (rec *Chown) SafeExec(root string) error {
	err := xattr.Setxattr(path.Join(root, rec.Path), XattrPrefix+"user", []byte(rec.UID))
	if err == nil {
		xattr.Setxattr(path.Join(root, rec.Path), XattrPrefix+"group", []byte(rec.GID))
	}
	return err
}

func (rec *Chown) String() string {
	return fmt.Sprintf("%T: filename = %s, uid = %s, gid = %s", rec, rec.Path, rec.UID, rec.GID)
}

// Key is implementation of Record interface
func (rec *Chown) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Chown) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Chown) Type() Type {
	return TypeChown
}

// ToBinary is implementation of Record interface
func (rec *Chown) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeString(target, rec.UID)
	if err != nil {
		return err
	}

	err = writeString(target, rec.GID)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Chown) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	s, err := readString(src)
	if err != nil {
		return err
	}
	rec.UID = s

	s, err = readString(src)
	if err != nil {
		return err
	}
	rec.GID = s

	return nil
}

// Chksum is implementation of Record interface
func (rec *Chown) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.UID)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.GID)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// CHMOD-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common chmod-related functionality.
// If there are special chmod actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewChmod returns a record object of type chmod from the parameters.
func NewChmod(filepath string, mode os.FileMode) Record {
	return &Chmod{
		Path: filepath,
		Mode: mode,
	}
}

// Exec is implementation of Record interface
func (rec *Chmod) Exec(root string) error {
	return os.Chmod(path.Join(root, rec.Path), rec.Mode)
}

// SafeExec is implementation of FakeRecord interface
func (rec *Chmod) SafeExec(root string) error {
	return xattr.Setxattr(path.Join(root, rec.Path), XattrPrefix+"mode", []byte(strconv.FormatUint(uint64(rec.Mode), 8)))
}

func (rec *Chmod) String() string {
	return fmt.Sprintf("%T: filename = %s, mode = %d", rec, rec.Path, rec.Mode)
}

// Key is implementation of Record interface
func (rec *Chmod) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Chmod) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Chmod) Type() Type {
	return TypeChmod
}

// ToBinary is implementation of Record interface
func (rec *Chmod) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Mode))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Chmod) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.Mode = os.FileMode(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Chmod) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.Mode))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// MTIME-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common mtime-related functionality.
// If there are special mtime actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewSetMtime returns a record object of type setmtime
func NewSetMtime(path string, mtime time.Time) Record {
	return &Setmtime{
		Path:  path,
		MTime: mtime.UnixNano(),
	}
}

// Exec is implementation of Record interface
func (rec *Setmtime) Exec(root string) error {
	//just change the atime to now. TODO: do we need to care about atime?
	return os.Chtimes(path.Join(root, rec.Path), time.Now().Local(), time.Unix(0, rec.MTime))
}

func (rec *Setmtime) String() string {
	return fmt.Sprintf("%T: filename = %s, mtime = %s", rec, rec.Path, time.Unix(0, rec.MTime))
}

// Key is implementation of Record interface
func (rec *Setmtime) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Setmtime) ExecType() ExecType {
	return DelayExec
}

// Type is implementation of Record interface
func (rec *Setmtime) Type() Type {
	return TypeSetmtime
}

// ToBinary is implementation of Record interface
func (rec *Setmtime) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.MTime))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Setmtime) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.MTime = int64(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Setmtime) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.MTime))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// SETXATTR-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common setxattr-related functionality.
// If there are special setxattr actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewSetXattr returns an record object of type setxattr from the parameters.
func NewSetXattr(path string, xattr string, val []byte) Record {
	return &Setxattr{
		Path:  path,
		Xattr: xattr,
		Data:  val,
	}
}

// Exec is implementation of Record interface
func (rec *Setxattr) Exec(root string) error {
	return xattr.Setxattr(path.Join(root, rec.Path), rec.Xattr, rec.Data)
}

// SafeExec is implementation of FakeRecord interface
func (rec *Setxattr) SafeExec(root string) error {
	return xattr.Setxattr(path.Join(root, rec.Path), XattrPrefix+"xattr."+rec.Xattr, rec.Data)
}

func (rec *Setxattr) String() string {
	return fmt.Sprintf("%T: path = %s, attr = %s, value = %s", rec, rec.Path, rec.Xattr, string(rec.Data))
}

// Key is implementation of Record interface
func (rec *Setxattr) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Setxattr) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Setxattr) Type() Type {
	return TypeSetxattr
}

// ToBinary is implementation of Record interface
func (rec *Setxattr) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Xattr)
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Data)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Setxattr) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	xattr, err := readString(src)
	if err != nil {
		return err
	}
	rec.Xattr = xattr

	data, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Data = data

	return nil
}

// Chksum is implementation of Record interface
func (rec *Setxattr) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.Xattr)
	if err != nil {
		return nil, err
	}

	err = hashBytes(h, rec.Data)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// RMXATTR-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common rmxattr-related functionality.
// If there are special rmxattr actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewRmXattr returns an record object of type rmxattr from the parameters.
func NewRmXattr(path string, xattr string) Record {
	return &Rmxattr{
		Path:  path,
		Xattr: xattr,
	}
}

// Exec is implementation of Record interface
func (rec *Rmxattr) Exec(root string) error {
	return xattr.Removexattr(path.Join(root, rec.Path), rec.Xattr)
}

// SafeExec is implementation of FakeRecord interface
func (rec *Rmxattr) SafeExec(root string) error {
	return xattr.Removexattr(path.Join(root, rec.Path), XattrPrefix+"xattr."+rec.Xattr)
}

func (rec *Rmxattr) String() string {
	return fmt.Sprintf("%T: path = %s, attr = %s", rec, rec.Path, rec.Xattr)
}

// Key is implementation of Record interface
func (rec *Rmxattr) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Rmxattr) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Rmxattr) Type() Type {
	return TypeRmxattr
}

// ToBinary is implementation of Record interface
func (rec *Rmxattr) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Xattr)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Rmxattr) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	xattr, err := readString(src)
	if err != nil {
		return err
	}
	rec.Xattr = xattr

	return nil
}

// Chksum is implementation of Record interface
func (rec *Rmxattr) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.Xattr)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// MKNOD-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common mknod-related functionality.
// If there are special mknod actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewMknod returns an record object of type mknod from the parameters.
func NewMknod(filepath string, mode uint32, dev int) Record {
	return &Mknod{
		Path: filepath,
		Mode: mode,
		Dev:  dev,
	}
}

// Exec is implementation of Record interface
func (rec *Mknod) Exec(root string) error {
	fullpath := path.Join(root, rec.Path)
	err := syscall.Mknod(fullpath, rec.Mode, rec.Dev)
	if err != nil {
		log.Println("Error: Mknod failed: ", err)
		return err
	}

	//our previous Mknod call could have set incorrect mode bits due to umask
	//so issue an explicit chmod
	err = syscall.Chmod(fullpath, rec.Mode)
	if err != nil {
		log.Println("Error: Mknod failed when doing chmod: ", err)
		return err

	}

	return nil
}

// SafeExec is implementation of FakeRecord interface
func (rec *Mknod) SafeExec(root string) error {
	// create a plain file and store special properties into its attribues
	fullpath := path.Join(root, rec.Path)
	fp, err := os.Create(fullpath)
	if err != nil {
		return err
	}
	fp.Close()
	err = xattr.Setxattr(fullpath, XattrPrefix+"dev", []byte(strconv.Itoa(rec.Dev)))
	if err == nil {
		err = xattr.Setxattr(fullpath, XattrPrefix+"devmode", []byte(strconv.FormatUint(uint64(rec.Mode), 8)))
	}
	return err
}

func (rec *Mknod) String() string {
	return fmt.Sprintf("%T: filename = %s, mode = %d, dev = %d", rec, rec.Path, rec.Mode, rec.Dev)
}

// Key is implementation of Record interface
func (rec *Mknod) Key() string {
	return rec.Path
}

// ExecType is implementation of Record interface
func (rec *Mknod) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec Mknod) Type() Type {
	return TypeMknod
}

// ToBinary is implementation of Record interface
func (rec *Mknod) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Path)
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Mode))
	if err != nil {
		return err
	}

	err = writeUint64(target, uint64(rec.Dev))
	return err
}

// FromBinary is implementation of Record interface
func (rec *Mknod) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.Path = path

	n, err := readUint64(src)
	if err != nil {
		return err
	}
	rec.Mode = uint32(n)

	n, err = readUint64(src)
	if err != nil {
		return err
	}
	rec.Dev = int(n)

	return nil
}

// Chksum is implementation of Record interface
func (rec *Mknod) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Path)
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.Mode))
	if err != nil {
		return nil, err
	}

	err = hashUint64(h, uint64(rec.Dev))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// RENAME-SPECIFIC INTERFACE IMPLEMENTATION/////////////////////////////////////////
// The functions below describe common rename-related functionality.
// If there are special rename actions required for a certain distribution,
// the functions below will need to be overriden by the code that is executing
// for that distribution.

// NewRename returns an record object of type rename from the parameters.
func NewRename(oldpath string, newpath string) Record {
	return &Rename{
		OldPath: oldpath,
		NewPath: newpath,
	}
}

// Exec is implementation of Record interface
// TODO: currently not used
func (rec *Rename) Exec(root string) error {
	return os.Rename(path.Join(root, rec.OldPath), path.Join(root, rec.NewPath))
}

func (rec *Rename) String() string {
	return fmt.Sprintf("%T: old path = %s, new path = %s ", rec, rec.OldPath, rec.NewPath)
}

// Key is implementation of Record interface
func (rec *Rename) Key() string {
	return rec.OldPath
}

// ExecType is implementation of Record interface
func (rec *Rename) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *Rename) Type() Type {
	return TypeRename
}

// ToBinary is implementation of Record interface
func (rec *Rename) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.OldPath)
	if err != nil {
		return err
	}

	err = writeString(target, rec.NewPath)
	return err
}

// FromBinary is implementation of Record interface
func (rec *Rename) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	path, err := readString(src)
	if err != nil {
		return err
	}
	rec.OldPath = path

	path, err = readString(src)
	if err != nil {
		return err
	}
	rec.NewPath = path

	return nil
}

// Chksum is implementation of Record interface
func (rec *Rename) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.OldPath)
	if err != nil {
		return nil, err
	}

	err = hashString(h, rec.NewPath)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// NewEOT returns an record object of type EOT
func NewEOT() Record {
	return &EOT{
		Name: "EOT",
	}
}

// Exec is the implementation of Record interface
func (rec *EOT) Exec(root string) error {
	return nil
}

// String is the implementation of Record interface
func (rec *EOT) String() string {
	return fmt.Sprintf("%T: name = %s", rec, rec.Name)
}

// Key is implementation of Record interface
func (rec *EOT) Key() string {
	return rec.Name
}

// ExecType is implementation of Record interface
func (rec *EOT) ExecType() ExecType {
	return SyncExec
}

// Type is implementation of Record interface
func (rec *EOT) Type() Type {
	return TypeEOT
}

// ToBinary is implementation of Record interface
func (rec *EOT) ToBinary(target io.Writer) error {
	err := writeUint64(target, uint64(rec.Type()))
	if err != nil {
		return err
	}

	err = writeBytes(target, rec.Hdr.Chksum)
	if err != nil {
		return err
	}

	err = writeString(target, rec.Name)
	return err
}

// FromBinary is implementation of Record interface
func (rec *EOT) FromBinary(src io.Reader) error {
	chksum, err := readBytes(src)
	if err != nil {
		return err
	}
	rec.Hdr.Chksum = chksum

	name, err := readString(src)
	if err != nil {
		return err
	}
	rec.Name = name

	return nil
}

// Chksum is implementation of Record interface
func (rec *EOT) Chksum(hf dlhash.Factory) ([]byte, error) {
	h := hf.New()

	err := hashString(h, rec.Name)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// Binary encode/decode utility functions
func writeUint64(target io.Writer, v uint64) error {
	err := binary.Write(target, binary.LittleEndian, v)
	return err
}

func readUint64(src io.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(src, binary.LittleEndian, &v)
	return v, err
}

func writeString(target io.Writer, s string) error {
	err := writeUint64(target, uint64(len(s)))
	if err != nil {
		return err
	}
	n, err := target.Write([]byte(s))
	if n != len(s) {
		return errors.New("Failed to encode string, wrong length encoded")
	}
	return err
}

func readString(src io.Reader) (string, error) {
	l, err := readUint64(src)
	if err != nil {
		return "", err
	}
	data := make([]byte, l, l)
	n, err := io.ReadAtLeast(src, data, int(l))
	if uint64(n) != l {
		return "", errors.New("Failed to decode string, wrong length")
	}
	return string(data), nil
}

func writeBytes(target io.Writer, data []byte) error {
	err := writeUint64(target, uint64(len(data)))
	if err != nil {
		return err
	}
	n, err := target.Write(data)
	if n != len(data) {
		return errors.New("Failed to encode []byte, wrong length encoded")
	}
	return err
}

func readBytes(src io.Reader) ([]byte, error) {
	l, err := readUint64(src)
	if err != nil {
		return nil, err
	}
	data := make([]byte, l, l)
	n, err := io.ReadAtLeast(src, data, int(l))
	if uint64(n) != l {
		return nil, errors.New("Failed to decode []byte, wrong length")
	}
	return data, nil
}

func hashUint64(h hash.Hash, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	_, err := h.Write(buf[:n])
	return err
}

func hashString(h hash.Hash, s string) error {
	_, err := h.Write([]byte(s))
	return err
}

func hashBytes(h hash.Hash, data []byte) error {
	_, err := h.Write(data)
	return err
}

// Send pushs a record into the channel which is consumed by a reader who send the records to the wire.
// Checksum is calculated before putting the record into the channel so checksum is transferred along.
func Send(rec Record, records chan<- Record, hf dlhash.Factory) error {
	chksum, err := rec.Chksum(hf)
	if err != nil {
		return err
	}

	rec.SetChksum(chksum)
	records <- rec
	return nil
}

// VerifyChksum verifies the checksum embedded inside the record is the same as a fresh calculation
func VerifyChksum(rec Record, hf dlhash.Factory) (bool, error) {
	chksum, err := rec.Chksum(hf)
	if err != nil {
		return false, err
	}

	return reflect.DeepEqual(rec.GetChksum(), chksum), nil
}
