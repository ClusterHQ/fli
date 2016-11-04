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

package sqlite3storage

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/ClusterHQ/fli/dp/datasrvstore"
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/dp/sync"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/bush"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/util"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/securefilepath"

	// So CLI and round trip tests can run properly
	_ "github.com/mattn/go-sqlite3"
)

// Sqlite3Storage ...
type Sqlite3Storage struct {
	path securefilepath.SecureFilePath
	db   *sql.DB
}

var (
	sqlDriverName = "sqlite3"

	_ metastore.Store    = &Sqlite3Storage{}
	_ datasrvstore.Store = &Sqlite3Storage{}
)

// TXLOCKING - transaction locking. Can be 'auto', 'immediate' or 'exclusive'.
// 'immediate' means no other writes to db once the transaction is in progress.
// When we create our db with 'immediate' flag, all transactions will be in 'immediate' mode.
// Basically, that buys us locking across the query+upate operations if they are
// part of the same transaction. On the postgress side, the equivalent is
// the "QUERY FOR UPDATE" clause.
const TXLOCKING = "immediate"

// Create makes a brand new SQLite3-based metadata storage database at the
// given path or fails if something exists at that path already.
func Create(path securefilepath.SecureFilePath) (*Sqlite3Storage, error) {
	// TODO Fix the race condition
	exists, err := path.Exists()
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.Errorf("Cannot create SQLite3-based data plane storage at %v because something exists there already.", path.Path())
	}
	db, err := sql.Open(sqlDriverName, "file://"+path.Path()+"?"+"_txlock="+TXLOCKING)
	if err != nil {
		return nil, errors.Errorf("Cannot create SQLite3-based data plane storage at %s: %v", path.Path(), err)
	}
	// Avoid multiple connections to the database.  Primarily, this is
	// interesting because it ensures all interactions happen over a single
	// connection.  This is only necessary when testing against an
	// in-memory database to which multiple connections cannot actually be
	// established (each connection is to a new, empty database).  It may
	// make more sense to somehow set up this state in the test suite but I
	// don't see a simple way to do that right now.  Also, the anticipate
	// usage patterns for this code, client-side dataplane work, don't
	// obviously benefit from multiple connections anyway.
	db.SetMaxOpenConns(1)
	err = createSchema(db)
	if err != nil {
		return nil, err
	}

	return &Sqlite3Storage{
		path: path,
		db:   db,
	}, nil
}

// Note: Time stamp is stored as int64
func createSchema(db *sql.DB) error {
	statements := []string{`
PRAGMA page_size = 4096
`, `
CREATE TABLE [volumeset] (
    [id] text,
    [creation_time] integer,
    [creator_username] text,
    [creator_uuid] text,
    [owner_username] text,
    [owner_uuid] text,
    [size] integer,
    [last_modified_time] integer,
    PRIMARY KEY([id])
)`, `
CREATE TABLE [snapshot] (
    [volumeset_id] text,
    [id] text,
    [parent_id] text,
    [blob_id] text,
    [creation_time] integer,
    [creator_username] text,
    [creator_uuid] text,
    [owner_username] text,
    [owner_uuid] text,
    [size] integer,
    [last_modified_time] integer,
    [depth] integer,
    PRIMARY KEY([volumeset_id], [id]),
    FOREIGN KEY([volumeset_id]) REFERENCES [volumeset]([id]),
    FOREIGN KEY([parent_id]) REFERENCES [snapshot]([id])
)`, `
CREATE TABLE [attributes] (
    [volumeset_id] text,
    [id] text,
    [key] text NOT NULL,
    [value] text NOT NULL,
    PRIMARY KEY([volumeset_id], [id], [key]),
    FOREIGN KEY([volumeset_id]) REFERENCES [volumeset]([id])
)`, `
CREATE TABLE [bush] (
    [volumeset_id] text,
    [id] text,
    [dsid] integer,
    PRIMARY KEY([id]),
    FOREIGN KEY([volumeset_id]) REFERENCES [volumeset]([id])
)`, `
CREATE TABLE [branch] (
    [id] text,
    [volumeset_id] text,
    [name] text,
    [tip] text,
    PRIMARY KEY([id]),
    UNIQUE([volumeset_id], [name]),
    UNIQUE([tip]),
    FOREIGN KEY([volumeset_id]) REFERENCES [volumeset]([id]),
    FOREIGN KEY([tip]) REFERENCES [snapshot]([id])
)`, `
CREATE TABLE [dataserver] (
    [id] INTEGER PRIMARY KEY AUTOINCREMENT,
    [url] text NOT NULL UNIQUE,
    [current] int
)`, `
CREATE TABLE [volume] (
    [name] text,
    [volumeset_id] text,
    [id] text,
    [parent_id] text,
    [mount_path] text NOT NULL UNIQUE,
    [creation_time] integer,
    [size] integer,
    PRIMARY KEY([id]),
    FOREIGN KEY([volumeset_id]) REFERENCES [volumeset]([id]),
    FOREIGN KEY([parent_id]) REFERENCES [snapshot]([id])
 )`,
	}
	for _, statement := range statements {
		_, err := db.Exec(statement)
		if err != nil {
			return errors.New(err)
		}
	}
	return nil
}

// Open returns a storage object backed by an existing SQLite3-based metadata
// storage database at the given path or fails if there is no such database
// there.
func Open(path securefilepath.SecureFilePath) (metastore.Client, error) {
	// TODO Fix the race condition
	exists, err := path.Exists()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.Errorf("Cannot open data-plane storage at %v because it does not exist.", path.Path())
	}
	db, err := sql.Open(sqlDriverName, "file://"+path.Path()+"?"+"_txlock="+TXLOCKING)
	if err != nil {
		return nil, errors.Errorf("Cannot create data-plane storage at %v: %v", path.Path(), err)
	}

	return &Sqlite3Storage{
		path: path,
		db:   db,
	}, nil
}

// DeleteVolumeSet ...
func (store *Sqlite3Storage) DeleteVolumeSet(id volumeset.ID) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	deleteVolumeSet, err := tx.Prepare(`
DELETE FROM [volumeset]
WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer deleteVolumeSet.Close()

	deleteSnapshots, err := tx.Prepare(`
DELETE FROM [snapshot]
WHERE [volumeset_id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer deleteSnapshots.Close()

	deleteSnapshotDescription, err := tx.Prepare(`
DELETE FROM [attributes]
WHERE [volumeset_id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer deleteSnapshotDescription.Close()

	deleteBranches, err := tx.Prepare(`
DELETE FROM [branch]
WHERE [volumeset_id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer deleteBranches.Close()

	deleteBushes, err := tx.Prepare(`
DELETE FROM [bush]
WHERE [volumeset_id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer deleteBushes.Close()

	delVolume, err := tx.Prepare(`
DELETE FROM [volume]
WHERE [volumeset_id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer delVolume.Close()

	for _, stmt := range []*sql.Stmt{
		deleteBranches,
		deleteSnapshotDescription,
		deleteSnapshots,
		deleteBushes,
		delVolume,
		deleteVolumeSet,
	} {
		_, err := stmt.Exec(id.String())
		if err != nil {
			return errors.New(err)
		}
	}
	return nil
}

// ImportVolumeSet ...
func (store *Sqlite3Storage) ImportVolumeSet(vs *volumeset.VolumeSet) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	insVolSet, err := tx.Prepare(`
INSERT INTO [volumeset] ([id], [creation_time], [creator_username], [creator_uuid], [owner_username], [owner_uuid], [size], [last_modified_time])
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`)
	if err != nil {
		return errors.New(err)
	}

	defer insVolSet.Close()
	_, err = insVolSet.Exec(
		vs.ID.String(),
		vs.CreationTime.UnixNano(),
		vs.CreatorUsername,
		vs.Creator,
		vs.OwnerUsername,
		vs.Owner,
		vs.Size,
		vs.LastModifiedTime.UnixNano(),
	)
	if err != nil {
		if err.Error() == "UNIQUE constraint failed: volumeset.id" {
			return &metastore.ErrVolumeSetAlreadyExists{}
		}

		return errors.New(err)
	}

	vs.StoreKnownKeys()
	err = insertAttrs(tx, vs.ID.String(), vs.ID.String(), vs.Attrs)
	vs.RetrieveKnownKeys()
	return err
}

// GetSnapshotIDs returns a list of snapshot IDs of a volume set.
func (store *Sqlite3Storage) GetSnapshotIDs(vsid volumeset.ID) ([]snapshot.ID, error) {
	tx, err := store.db.Begin()
	if err != nil {
		return nil, errors.New(err)
	}
	defer tx.Rollback()

	ids := []snapshot.ID{}
	stmt := "SELECT [id] FROM [snapshot] WHERE [volumeset_id] = ?"
	sel, err := tx.Prepare(stmt)
	if err != nil {
		return ids, errors.Errorf("GetSnapshots prepare query failed: %v", err)
	}
	rows, err := sel.Query(vsid.String())
	if err != nil {
		return ids, errors.Errorf("Failed to query database with statement '%s': %v", stmt, err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			return ids, errors.New(err)
		}
		ids = append(ids, snapshot.NewID(id))
	}

	return ids, nil
}

// GetSnapshots returns a list of snapshots ordered by specifications in passed-in query.
// Caller is responsible for actually filtering output based on query.
func (store *Sqlite3Storage) GetSnapshots(q snapshot.Query) ([]*snapshot.Snapshot, error) {
	tx, err := store.db.Begin()
	if err != nil {
		return nil, errors.New(err)
	}
	defer tx.Rollback()

	snaps := []*snapshot.Snapshot{}
	statement := `
	SELECT s.[volumeset_id], s.[id], [parent_id], [blob_id], [creation_time], [creator_username], [creator_uuid],
	[owner_username], [owner_uuid], [size],
	[last_modified_time], [depth], b.[tip], b.[name]
	FROM [snapshot] s
	LEFT JOIN [branch] b ON b.[volumeset_id] = s.[volumeset_id] AND b.[tip] = s.[id]
	`
	params := []interface{}{}
	if !q.ID.IsNilID() {
		statement += "WHERE s.[id]=?"
		params = append(params, q.ID.String())
	}

	// Sqlite3 returned error "too many parameters" when it is over a few hundreds IDs(500 worked, 1,000 failed).
	// Since Sqlite3 is used locally, it is not a security issue as much as in postgres, using direct ID string instead
	// of 'IN'.
	if len(q.IDs) != 0 {
		statement += " WHERE s.id IN ("
		for idx, id := range q.IDs {
			if idx != 0 {
				statement += ", "
			}
			statement += "\"" + id.String() + "\""
		}
		statement += ")"
	}

	statement += " ORDER BY"
	switch q.SortBy {
	case snapshot.OrderBySize:
		statement += " [size] "
	case snapshot.OrderByTime:
		statement += " [creation_time] "
	default:
		statement += " s.[id] "
	}

	switch q.OrderType {
	case "":
	case volumeset.ASC:
		statement += volumeset.ASC
	case volumeset.DESC:
		statement += volumeset.DESC
	default:
		return nil, errors.Errorf("Illegal Order Type value: %s", q.OrderType)
	}

	limit := q.Limit
	if limit <= 0 {
		// -1 is sqlite value for no limit. We need to have this since offset
		// is not understood without limit.
		limit = -1
	}
	statement += " LIMIT ? OFFSET ?"
	params = append(params, limit, q.Offset)

	selSnaps, err := tx.Prepare(statement)
	if err != nil {
		return nil, errors.Errorf("GetSnapshots prepare query failed: %v", err)
	}

	rows, err := selSnaps.Query(params...)
	if err != nil {
		return nil, errors.Errorf("Failed to query database with statement '%s': %v", statement, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			vsid             string
			sid              string
			parentID         sql.NullString
			blobid           string
			creationTime     int64
			creatorName      string
			creatorUUID      string
			ownerName        string
			ownerUUID        string
			size             uint64
			parentIDPtr      *snapshot.ID
			lastModifiedTime int64
			depth            int
			tip              sql.NullString
			branchName       sql.NullString
		)
		err = rows.Scan(
			&vsid, &sid, &parentID, &blobid, &creationTime, &creatorName, &creatorUUID,
			&ownerName, &ownerUUID, &size, &lastModifiedTime,
			&depth, &tip, &branchName,
		)

		if err == sql.ErrNoRows {
			return nil, &metastore.ErrSnapshotNotFound{}
		}

		if err != nil {
			return nil, errors.New(err)
		}

		if parentID.Valid {
			pid := snapshot.NewID(parentID.String)
			parentIDPtr = &pid
		}

		s := snapshot.Snapshot{
			VolSetID:         volumeset.NewID(vsid),
			ID:               snapshot.NewID(sid),
			ParentID:         parentIDPtr,
			CreationTime:     time.Unix(0, creationTime),
			LastModifiedTime: time.Unix(0, lastModifiedTime),
			Creator:          creatorUUID,
			CreatorName:      creatorName,
			Owner:            ownerUUID,
			OwnerName:        ownerName,
			Size:             size,
			Depth:            depth,
			BlobID:           blob.NewID(blobid),
			PrevBlobID:       blob.NewID(blobid),
		}

		if tip.Valid {
			s.IsTip = true
			if branchName.Valid {
				s.BranchName = branchName.String
			}
		}

		snaps = append(snaps, &s)
	}

	// Update meta fields
	for _, ss := range snaps {
		attr, err := getAttrs(tx, ss.VolSetID.String(), ss.ID.String())
		if err != nil {
			return nil, err
		}
		ss.Attrs = attr
		ss.RetrieveKnownKeys()

		numChildren, err := snapGetNumChildren(tx, ss.ID)
		if err != nil {
			return nil, err
		}
		ss.NumChildren = numChildren
	}

	return snaps, nil
}

// internal function that is used by the caller that has already initiated the transaction.
func getVolumeSets(tx *sql.Tx, q volumeset.Query) ([]*volumeset.VolumeSet, error) {
	var (
		rows *sql.Rows
		err  error
	)

	// Note: Unlike in postgres, using left join with attributes turns out slower than without it.
	statement :=
		"SELECT [id], [creation_time], [creator_username], [creator_uuid], [owner_username], [owner_uuid], [size], [last_modified_time], " +
			"(SELECT count(id) AS [branchCnt] FROM [branch] AS b WHERE b.volumeset_id = v.id), " +
			"(SELECT count(id) AS [snapCnt] FROM [snapshot] AS s WHERE s.volumeset_id = v.id), " +
			"(SELECT max([creation_time]) AS [lastSnap] FROM [snapshot] AS ss WHERE ss.volumeset_id = v.id) " +
			"FROM [volumeset] AS v"

	params := []interface{}{}
	if !q.ID.IsNilID() {
		statement += " WHERE id=?"
		params = append(params, q.ID.String())
	}

	// Sqlite3 returned error "too many parameters" when it is over a few hundreds IDs(500 worked, 1,000 failed).
	// Since Sqlite3 is used locally, it is not a security issue as much as in postgres, using direct ID string instead
	// of 'IN'.
	if len(q.IDs) != 0 {
		statement += " WHERE id IN ("
		for idx, id := range q.IDs {
			if idx != 0 {
				statement += ", "
			}
			statement += "\"" + id.String() + "\""
		}
		statement += ")"
	}

	statement += " ORDER BY "
	switch q.SortBy {
	case volumeset.OrderBySize:
		statement += "[size] "
	case volumeset.OrderByTime:
		statement += "[creation_time] "
	default:
		statement += "[id] "
	}

	switch q.OrderType {
	case "":
	case volumeset.ASC:
		statement += volumeset.ASC
	case volumeset.DESC:
		statement += volumeset.DESC
	default:
		return nil, errors.Errorf("Illegal Order Type value: %s", q.OrderType)
	}

	limit := q.Limit
	if limit <= 0 {
		// -1 is sqlite value for no limit. We need to have this since offset
		// is not understood without limit.
		limit = -1
	}
	statement += " LIMIT ? OFFSET ?"
	params = append(params, limit, q.Offset)
	stmt, err := tx.Prepare(statement)
	if err != nil {
		return nil, errors.Errorf("VolumeSet prepare query failed: %v", err)
	}

	rows, err = stmt.Query(params...)
	if err != nil {
		return nil, errors.Errorf("VolumeSet query failed: %v", err)
	}
	defer rows.Close()

	vss := []*volumeset.VolumeSet{}
	for rows.Next() {
		var (
			id               string
			creationTime     int64
			creatorName      string
			creatorUUID      string
			ownerName        string
			ownerUUID        string
			size             uint64
			lastModifiedTime int64
			branchCnt        int
			snapCnt          int
			timeStr          sql.NullString
		)

		err = rows.Scan(&id, &creationTime, &creatorName, &creatorUUID, &ownerName, &ownerUUID, &size,
			&lastModifiedTime, &branchCnt, &snapCnt, &timeStr)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, errors.Errorf("Failed to read from rows: %v\n", err)
		}

		var t time.Time
		if timeStr.Valid {
			timeInt64, err := strconv.ParseInt(timeStr.String, 10, 64)
			if err != nil {
				return nil, errors.Errorf("Failed to parse time string '%v': %v", timeStr, err)
			}
			t = time.Unix(0, timeInt64)
		}

		vss = append(vss, &volumeset.VolumeSet{
			ID:               volumeset.NewID(id),
			CreationTime:     time.Unix(0, creationTime),
			Creator:          creatorUUID,
			CreatorUsername:  creatorName,
			Owner:            ownerUUID,
			OwnerUsername:    ownerName,
			Size:             size,
			LastModifiedTime: time.Unix(0, lastModifiedTime),
			NumBranches:      branchCnt,
			NumSnapshots:     snapCnt,
			LastSnapshotTime: t,
		})
	}

	// Note: Join volumeset table and attributes table didn't produce faster results. It is actually slower.
	// Updates non persisted fields of volume set
	// Note: Postgres can't run embedded queries, updates other fields outside the main query
	for _, vs := range vss {
		attr, err := getAttrs(tx, vs.ID.String(), vs.ID.String())
		if err != nil {
			return nil, err
		}
		vs.Attrs = attr
		vs.RetrieveKnownKeys()
	}

	return vss, nil

}

// GetVolumeSets ...
func (store *Sqlite3Storage) GetVolumeSets(q volumeset.Query) ([]*volumeset.VolumeSet, error) {
	tx, err := store.db.Begin()
	if err != nil {
		return nil, errors.New(err)
	}
	defer tx.Rollback()

	volsets, err := getVolumeSets(tx, q)
	return volsets, err
}

// GetTip ...
func (store *Sqlite3Storage) GetTip(vsid volumeset.ID, branch string) (*snapshot.Snapshot, error) {
	if err := metastore.ValidateBranchName(branch); err != nil {
		return nil, err
	}

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	tip, err := getTip(tx, branch, vsid)
	if err != nil {
		return nil, err
	}
	if tip == nil {
		return nil, &metastore.ErrBranchNotFound{}
	}
	return getSnapshot(tx, *tip)
}

// GetBranches ...
func (store *Sqlite3Storage) GetBranches(q branch.Query) ([]*branch.Branch, error) {
	tx, err := store.db.Begin()
	if err != nil {
		return nil, errors.New(err)
	}
	defer tx.Rollback()
	var args []interface{}

	statement := `
SELECT [id], [name], [tip]
FROM [branch]
WHERE [volumeset_id] = ?
`
	args = append(args, q.VolSetID.String())
	if !q.ID.IsNilID() {
		statement += "AND [id] = ?"
		args = append(args, q.ID.String())
	}
	selectBranches, err := tx.Prepare(statement)
	if err != nil {
		return nil, errors.New(err)
	}
	defer selectBranches.Close()

	rows, err := selectBranches.Query(args...)
	if err != nil {
		return nil, errors.New(err)
	}
	defer rows.Close()

	result := []*branch.Branch{}
	for rows.Next() {
		var (
			id         string
			branchName string
			tip        string
			name       sql.NullString
		)
		err = rows.Scan(&id, &name, &tip)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, errors.New(err)
		}

		if name.Valid {
			branchName = name.String
		} else {
			branchName = ""
		}
		snap, err := getSnapshot(tx, snapshot.NewID(tip))
		if err != nil {
			return nil, err
		}

		result = append(
			result,
			&branch.Branch{
				ID:       branch.NewID(id),
				Name:     branchName,
				Tip:      snap,
				HasChild: false,
			},
		)
	}

	// Update tip's has child field
	for _, b := range result {
		numChildren, err := snapGetNumChildren(tx, b.Tip.ID)
		if err != nil {
			return nil, err
		}
		b.HasChild = numChildren > 0
	}

	return result, nil
}

// RenameBranch ...
func (store *Sqlite3Storage) RenameBranch(vsid volumeset.ID, oldName, newName string) error {
	if err := metastore.ValidateBranchName(newName); err != nil {
		return err
	}

	if oldName == newName {
		return errors.Errorf(
			"RenameBranch(%v, %v) disallowed because branch exists(%v)", oldName, oldName, oldName,
		)
	}

	updateBranch, err := store.db.Prepare(`
UPDATE [branch]
SET [name] = ?
WHERE [volumeset_id] = ? AND [name] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer updateBranch.Close()

	result, err := updateBranch.Exec(newName, vsid.String(), oldName)
	if err != nil {
		return errors.New(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}
	if rowsAffected == 0 {
		return errors.Errorf(
			"Could not rename %v to %v: no such branch %v", oldName, newName, oldName,
		)
	}
	if rowsAffected > 1 {
		return errors.Errorf(
			"RenameBranch(%v, %v) affected %v rows: branch table is corrupt.", oldName, newName, rowsAffected,
		)
	}
	return nil
}

// ImportVolume ...
func (store *Sqlite3Storage) ImportVolume(vol *volume.Volume) error {
	var parent interface{}
	if vol.BaseID != nil {
		parent = vol.BaseID.String()
	}

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	insVol, err := tx.Prepare(`
INSERT INTO [volume] ([volumeset_id], [id], [parent_id], [mount_path], [creation_time], [size], [name]) VALUES (?, ?, ?, ?, ?, ?, ?)
`)
	if err != nil {
		return errors.New(err)
	}
	defer insVol.Close()

	_, err = insVol.Exec(
		vol.VolSetID.String(),
		vol.ID.String(),
		parent,
		vol.MntPath.Path(),
		vol.CreationTime.UnixNano(),
		vol.Size,
		vol.Name,
	)
	if err != nil {
		return errors.New(err)
	}

	err = insertAttrs(tx, vol.VolSetID.String(), vol.ID.String(), vol.Attrs)
	if err != nil {
		return err
	}

	return nil
}

// GetVolume ...
func (store *Sqlite3Storage) GetVolume(vid volume.ID) (*volume.Volume, error) {
	var (
		parent       sql.NullString
		vsid         string
		parentID     *snapshot.ID
		path         string
		creationTime int64
		size         uint64
		name         string
	)

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	selVol, err := tx.Prepare(`
SELECT [volumeset_id], [parent_id], [mount_path], [creation_time], [size], [name]
FROM [volume]
WHERE [id] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer selVol.Close()

	err = selVol.QueryRow(vid.String()).Scan(&vsid, &parent, &path, &creationTime, &size, &name)
	if err == sql.ErrNoRows {
		return nil, &metastore.ErrVolumeNotFound{}
	}
	if err != nil {
		return nil, errors.New(err)
	}

	if parent.Valid {
		id := snapshot.NewID(parent.String)
		parentID = &id
	}

	mntPath, err := securefilepath.New(path)
	if err != nil {
		return nil, err
	}

	attr, err := getAttrs(tx, vsid, vid.String())
	if err != nil {
		return nil, err
	}

	return &volume.Volume{
		ID:           vid,
		VolSetID:     volumeset.NewID(vsid),
		BaseID:       parentID,
		MntPath:      mntPath,
		Attrs:        attr,
		CreationTime: time.Unix(0, creationTime),
		Size:         size,
		Name:         name,
	}, nil
}

// DeleteVolume ...
func (store *Sqlite3Storage) DeleteVolume(vid volume.ID) error {
	var vsid string

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	selVol, err := tx.Prepare(`
SELECT [volumeset_id]
FROM [volume]
WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer selVol.Close()

	delVol, err := tx.Prepare(`
DELETE FROM [volume] where [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer delVol.Close()

	// Find out volume set id
	err = selVol.QueryRow(vid.String()).Scan(&vsid)
	if err == sql.ErrNoRows {
		return &metastore.ErrVolumeNotFound{}
	}
	if err != nil {
		return err
	}

	err = delAttrs(tx, vsid, vid.String())
	if err != nil {
		return err
	}

	_, err = delVol.Exec(vid.String())
	if err != nil {
		return errors.New(err)
	}

	return nil
}

// GetVolumes ...
func (store *Sqlite3Storage) GetVolumes(vsid volumeset.ID) ([]*volume.Volume, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	secVols, err := tx.Prepare(`
SELECT [volumeset_id], [id], [parent_id], [mount_path], [creation_time], [size], [name]
FROM [volume]
WHERE [volumeset_id] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer secVols.Close()

	rows, err := secVols.Query(vsid.String())
	if err != nil {
		return nil, errors.New(err)
	}
	defer rows.Close()

	var vols []*volume.Volume
	for rows.Next() {
		var (
			parent       sql.NullString
			vs           string
			vol          string
			parentID     *snapshot.ID
			path         string
			creationTime int64
			size         uint64
			name         string
		)
		err := rows.Scan(&vs, &vol, &parent, &path, &creationTime, &size, &name)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, errors.New(err)
		}

		if parent.Valid {
			id := snapshot.NewID(parent.String)
			parentID = &id
		} else {
			parentID = nil
		}

		mntPath, err := securefilepath.New(path)
		if err != nil {
			return nil, err
		}

		attr, err := getAttrs(tx, vs, vol)
		if err != nil {
			return nil, err
		}

		vols = append(vols,
			&volume.Volume{
				ID:           volume.NewID(vol),
				VolSetID:     volumeset.NewID(vs),
				BaseID:       parentID,
				MntPath:      mntPath,
				Attrs:        attr,
				CreationTime: time.Unix(0, creationTime),
				Size:         size,
				Name:         name,
			})
	}

	return vols, nil
}

// UpdateSnapshots implements metastore interface
func (store *Sqlite3Storage) UpdateSnapshots(snaps []*metastore.SnapshotPair) ([]metastore.SnapMetaConflict, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	var conflicts []metastore.SnapMetaConflict
	for _, snap := range snaps {
		c, err := store.snapshotResolveConflict(tx, snap.Cur, snap.Init)
		if err != nil {
			return nil, err
		}
		if !c.IsEmpty() {
			conflicts = append(conflicts, c)
		}
	}

	return conflicts, nil
}

// UpdateSnapshot implements metastore interface
func (store *Sqlite3Storage) UpdateSnapshot(snapCur, snapInit *snapshot.Snapshot) (metastore.SnapMetaConflict, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	return store.snapshotResolveConflict(tx, snapCur, snapInit)
}

// snapshotResolveConflict compares the three versions of a snapshot and decides what action to take(update,
// do nothing, or return conflict)
func (store *Sqlite3Storage) snapshotResolveConflict(tx *sql.Tx,
	snapCur, snapInit *snapshot.Snapshot) (metastore.SnapMetaConflict, error) {
	snapTgt, err := getSnapshot(tx, snapCur.ID)
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}

	if snapInit == nil {
		return metastore.SnapMetaConflict{}, updateSnapshot(tx, snapCur)
	}

	status := sync.CheckSnapConflict(snapTgt, snapCur, snapInit)
	switch status {
	case metastore.UseCurrent:
		return metastore.SnapMetaConflict{}, updateSnapshot(tx, snapCur)
	case metastore.UseTgtConflict:
		return metastore.SnapMetaConflict{Tgt: snapTgt, Cur: snapCur, Init: snapInit}, nil
	}

	// Target is not updated
	return metastore.SnapMetaConflict{}, nil
}

// updateSnapshot updates an existing snapshot
func updateSnapshot(tx *sql.Tx, snap *snapshot.Snapshot) error {
	if !snap.PrevBlobID.Equals(snap.BlobID) {
		if !snap.PrevBlobID.IsNilID() && !snap.BlobID.IsNilID() {
			return errors.New("Already have blob")
		}

		updateBlobID, err := tx.Prepare(`
UPDATE [snapshot]
SET [blob_id] = ?, [size] = ?
WHERE [id] = ? AND [blob_id] = ?
`)
		if err != nil {
			return errors.New(err)
		}
		defer updateBlobID.Close()

		result, err := updateBlobID.Exec(snap.BlobID.String(), snap.Size, snap.ID.String(),
			snap.PrevBlobID.String())
		if err != nil {
			return errors.New(err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return errors.New(err)
		}

		if rowsAffected == 0 {
			return errors.Errorf(
				"Failed to update snapshot(%v)'s blob ID(%v): no such snapshot", snap.ID, snap.BlobID)
		}

		if rowsAffected > 1 {
			return errors.Errorf(
				"Failed to update snapshot(%v)'s blob ID(%v): found more than one snapshots",
				snap.ID, snap.BlobID)
		}
	}

	// TODO: Check if the fields are actually need to be updated?
	updateSnap, err := tx.Prepare(`
UPDATE [snapshot]
SET [last_modified_time] = ?
WHERE [id] = ?`)
	if err != nil {
		return errors.New(err)
	}
	defer updateSnap.Close()

	result, err := updateSnap.Exec(snap.LastModifiedTime.UnixNano(), snap.ID.String())
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	if rowsAffected == 0 {
		return errors.Errorf(
			"Failed to update snapshot (%v): no such snapshot", snap.ID,
		)
	}

	if rowsAffected > 1 {
		return errors.Errorf(
			"Failed to udpate snapshot (%v): found more than one snapshot", snap.ID,
		)
	}

	err = delAttrs(tx, snap.VolSetID.String(), snap.ID.String())
	if err != nil {
		return err
	}

	snap.StoreKnownKeys()
	err = insertAttrs(tx, snap.VolSetID.String(), snap.ID.String(), snap.Attrs)
	if err != nil {
		return err
	}
	snap.RetrieveKnownKeys()

	return nil
}

// UpdateVolume ...
func (store *Sqlite3Storage) UpdateVolume(vol *volume.Volume) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	updateVol, err := tx.Prepare(`
UPDATE [volume] SET [name] = ? WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer updateVol.Close()

	result, err := updateVol.Exec(vol.Name, vol.ID.String())
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	if rowsAffected == 0 {
		return errors.Errorf(
			"Failed to update volume(%v): no such volume exists", vol.ID)
	}

	// Updates attributes
	err = delAttrs(tx, vol.VolSetID.String(), vol.ID.String())
	if err != nil {
		return err
	}

	err = insertAttrs(tx, vol.VolSetID.String(), vol.ID.String(), vol.Attrs)
	if err != nil {
		return err
	}

	// Updates other mutable fields
	return nil
}

// UpdateVolumeSet ...
func (store *Sqlite3Storage) UpdateVolumeSet(
	vsCur, vsInit *volumeset.VolumeSet) (metastore.VSMetaConflict, error) {
	tx, err := store.db.Begin()
	if err != nil {
		return metastore.VSMetaConflict{}, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	volsets, err := getVolumeSets(tx, volumeset.Query{ID: vsCur.ID})
	if err != nil {
		return metastore.VSMetaConflict{}, err
	}

	if len(volsets) == 0 {
		return metastore.VSMetaConflict{}, &metastore.ErrVolumeSetNotFound{}
	}

	if vsInit == nil {
		return metastore.VSMetaConflict{}, updateVolumeSet(tx, vsCur)
	}

	vsTgt := volsets[0]
	status := sync.CheckVSConflict(vsTgt, vsCur, vsInit)
	switch status {
	case metastore.UseCurrent:
		return metastore.VSMetaConflict{}, updateVolumeSet(tx, vsCur)
	case metastore.UseTgtConflict:
		return metastore.VSMetaConflict{Tgt: vsTgt, Cur: vsCur, Init: vsInit}, nil
	}

	return metastore.VSMetaConflict{}, nil
}

// Internal function to update the volume set once transaction has already been initiated.
// Caller is expected to have initiated transaction, opened the volume set for update.
func updateVolumeSet(tx *sql.Tx, vs *volumeset.VolumeSet) error {
	updateVS, err := tx.Prepare(`
UPDATE [volumeset] SET [last_modified_time] = ?, [owner_username] = ?, [owner_uuid] = ?, [creator_username] = ?, [creator_uuid] = ? WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}

	defer updateVS.Close()

	result, err := updateVS.Exec(vs.LastModifiedTime.UnixNano(),
		vs.OwnerUsername, vs.Owner, vs.CreatorUsername, vs.Creator, vs.ID.String())
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	if rowsAffected == 0 {
		return errors.Errorf("Failed to update volumeset(%v): no such volumeset", vs.ID)
	}

	// Updates attributes
	err = delAttrs(tx, vs.ID.String(), vs.ID.String())
	if err != nil {
		return err
	}

	vs.StoreKnownKeys()
	err = insertAttrs(tx, vs.ID.String(), vs.ID.String(), vs.Attrs)
	if err != nil {
		return err
	}
	vs.RetrieveKnownKeys()

	return nil
}

// forkBranch ...
func (store *Sqlite3Storage) forkBranch(branchID branch.ID, branchName string,
	snapshots ...*snapshot.Snapshot) error {
	if err := metastore.ValidateBranchName(branchName); err != nil {
		return err
	}

	if len(snapshots) == 0 {
		return nil
	}

	// Verify all snapshots given belong to the same volumeset
	vsid := snapshots[0].VolSetID
	for _, sn := range snapshots {
		if sn.VolSetID != vsid {
			return errors.New("Fork failed because snapshots do not belong to the same volumeset")
		}
	}

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	depth := 0
	if snapshots[0].ParentID != nil {
		parent, err := getSnapshot(tx, *snapshots[0].ParentID)
		if err != nil {
			return err
		}
		depth = parent.Depth
	}

	for _, sn := range snapshots {
		depth++
		err = importSnapshot(tx, sn, depth)
		if err != nil {
			return err
		}
	}

	err = insertBranch(tx, snapshots[len(snapshots)-1], branchID, branchName)
	return err
}

// ImportBranch ...
func (store *Sqlite3Storage) ImportBranch(branchID branch.ID, branchName string,
	snapshots ...*snapshot.Snapshot) error {
	return store.forkBranch(branchID, branchName, snapshots...)
}

// ForkBranch ...
func (store *Sqlite3Storage) ForkBranch(branchName string, snapshots ...*snapshot.Snapshot) error {
	return store.forkBranch(branch.NewRandomID(), branchName, snapshots...)
}

// ExtendBranch ...
func (store *Sqlite3Storage) ExtendBranch(snapshots ...*snapshot.Snapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	// Verify all snapshots given belong to the same volumeset
	vsid := snapshots[0].VolSetID
	for _, sn := range snapshots {
		if sn.VolSetID != vsid {
			return errors.New("Extend branch failed because snapshots do not belong to the same volumeset")
		}
	}

	// Verify that the first snapshot's parent is not nil
	if snapshots[0].ParentID == nil {
		return errors.New("Extend branch failed cant' extend without a parent")
	}

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	parent, err := getSnapshot(tx, *snapshots[0].ParentID)
	if err != nil {
		return err
	}
	depth := parent.Depth

	for _, sn := range snapshots {
		depth++
		err = importSnapshot(tx, sn, depth)
		if err != nil {
			return err
		}
	}

	return updateTip(tx, *snapshots[0].ParentID, snapshots[len(snapshots)-1].ID)
}

// importSnapshot is the helper function for fork and extend branch; it adds new snapshots info to the proper tables.
func importSnapshot(tx *sql.Tx, sn *snapshot.Snapshot, depth int) error {
	insertSnapshot, err := tx.Prepare(`
INSERT INTO [snapshot] ([volumeset_id], [id], [parent_id], [blob_id], [creation_time], [creator_username],
[creator_uuid], [owner_username], [owner_uuid], [size], [last_modified_time], [depth])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`)
	if err != nil {
		return errors.New(err)
	}
	defer insertSnapshot.Close()

	var dbParentID interface{}

	if sn.ParentID != nil {
		dbParentID = sn.ParentID.String()
	}

	_, err = getSnapshot(tx, sn.ID)
	if err != nil {
		if _, ok := err.(*metastore.ErrSnapshotNotFound); !ok {
			return err
		}

		_, err = insertSnapshot.Exec(
			sn.VolSetID.String(),
			sn.ID.String(),
			dbParentID,
			sn.BlobID.String(),
			sn.CreationTime.UnixNano(),
			sn.CreatorName,
			sn.Creator,
			sn.OwnerName,
			sn.Owner,
			sn.Size,
			sn.LastModifiedTime.UnixNano(),
			depth,
		)
		if err != nil {
			return errors.New(err)
		}

		sn.StoreKnownKeys()
		err = insertAttrs(tx, sn.VolSetID.String(), sn.ID.String(), sn.Attrs)
		if err != nil {
			return err
		}
		// To remove known keys from the attributes
		sn.RetrieveKnownKeys()
	}

	return nil
}

func getSnapshot(tx *sql.Tx, id snapshot.ID) (*snapshot.Snapshot, error) {
	selectSnapshot, err := tx.Prepare(`
SELECT [id], [parent_id], [volumeset_id], [blob_id], [creation_time], [creator_username], [creator_uuid], [owner_username], [owner_uuid],  [size], [last_modified_time], [depth]
FROM [snapshot]
WHERE [id] = ?
`)
	if err != nil {
		return nil, err
	}
	defer selectSnapshot.Close()

	var (
		vsid             string
		sid              string
		parentID         sql.NullString
		blobid           string
		creationTime     int64
		creatorName      string
		creatorUUID      string
		ownerName        string
		ownerUUID        string
		size             uint64
		parentIDPtr      *snapshot.ID
		lastModifiedTime int64
		depth            int
	)
	err = selectSnapshot.QueryRow(id.String()).Scan(
		&sid, &parentID, &vsid, &blobid, &creationTime, &creatorName, &creatorUUID,
		&ownerName, &ownerUUID, &size, &lastModifiedTime, &depth,
	)
	if err == sql.ErrNoRows {
		return nil, &metastore.ErrSnapshotNotFound{}
	}
	if err != nil {
		return nil, errors.New(err)
	}

	if parentID.Valid {
		pid := snapshot.NewID(parentID.String)
		parentIDPtr = &pid
	}

	attr, err := getAttrs(tx, vsid, id.String())
	if err != nil {
		return nil, err
	}

	snapid := snapshot.NewID(sid)
	numChildren, err := snapGetNumChildren(tx, snapid)
	if err != nil {
		return nil, err
	}

	volsetid := volumeset.NewID(vsid)
	isTip, branchName, err := getTipBySnapshotID(tx, volsetid, snapid)
	if err != nil {
		return nil, err
	}

	snap := &snapshot.Snapshot{
		VolSetID:         volsetid,
		ID:               snapid,
		ParentID:         parentIDPtr,
		Attrs:            attr,
		CreationTime:     time.Unix(0, creationTime),
		LastModifiedTime: time.Unix(0, lastModifiedTime),
		Creator:          creatorUUID,
		CreatorName:      creatorName,
		Owner:            ownerUUID,
		OwnerName:        ownerName,
		Size:             size,
		Depth:            depth,
		BlobID:           blob.NewID(blobid),
		PrevBlobID:       blob.NewID(blobid),
		NumChildren:      numChildren,
		IsTip:            isTip,
		BranchName:       branchName,
	}
	snap.RetrieveKnownKeys()

	return snap, nil
}

// getTip looks up a tip by a branch's name, returns nil if tip does not exist
func getTip(tx *sql.Tx, branchName string, vsid volumeset.ID) (*snapshot.ID, error) {
	selectTip, err := tx.Prepare(`
SELECT [tip]
FROM [branch]
WHERE [volumeset_id] = ? AND [name] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer selectTip.Close()

	tip := ""
	err = selectTip.QueryRow(vsid.String(), branchName).Scan(&tip)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New(err)
	}
	tipS := snapshot.NewID(tip)
	return &tipS, nil
}

// getTipBySnapshotID looks up a tip by a snapshot's ID, returns true and the branch's name if the snapshot is a tip
func getTipBySnapshotID(tx *sql.Tx, vsid volumeset.ID, snapid snapshot.ID) (bool, string, error) {
	selectTip, err := tx.Prepare(`
SELECT [name]
FROM [branch]
WHERE [volumeset_id] = ? AND [tip] = ?
`)
	if err != nil {
		return false, "", errors.New(err)
	}
	defer selectTip.Close()

	var name sql.NullString
	err = selectTip.QueryRow(vsid.String(), snapid.String()).Scan(&name)
	if err == sql.ErrNoRows {
		return false, "", nil
	}

	if err != nil {
		return false, "", errors.New(err)
	}

	if name.Valid {
		return true, name.String, nil
	}

	return true, "", nil
}

func getAttrs(tx *sql.Tx, vsid string, id string) (attrs.Attrs, error) {
	selAttrs, err := tx.Prepare(`
SELECT [key], [value]
FROM [attributes]
WHERE [volumeset_id] = ? AND [id] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer selAttrs.Close()

	attr := make(attrs.Attrs, 0)
	rows, err := selAttrs.Query(vsid, id)
	if err == sql.ErrNoRows {
		return attr, nil
	}
	if err != nil {
		return nil, errors.New(err)
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, errors.New(err)
		}
		attr[key] = value
	}

	return attr, nil
}

func insertAttrs(tx *sql.Tx, vsid string, id string, attrs attrs.Attrs) error {
	if attrs == nil || len(attrs) == 0 {
		return nil
	}

	insAttrs, err := tx.Prepare(`
INSERT INTO [attributes] ([volumeset_id], [id], [key], [value])
VALUES (?, ?, ?, ?)
`)
	if err != nil {
		return errors.New(err)
	}
	defer insAttrs.Close()

	for key, value := range attrs {
		_, err = insAttrs.Exec(
			vsid,
			id,
			key,
			value,
		)
		if err != nil {
			return errors.New(err)
		}
	}

	return nil
}

func delAttrs(tx *sql.Tx, vsid string, id string) error {
	delAttrs, err := tx.Prepare(`
DELETE FROM [attributes]
WHERE [volumeset_id] = ? AND [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer delAttrs.Close()

	_, err = delAttrs.Exec(
		vsid,
		id,
	)

	if err != nil {
		return errors.New(err)
	}
	return nil
}

func updateTip(tx *sql.Tx, oldTip, newTip snapshot.ID) error {
	upd, err := tx.Prepare(`
UPDATE [branch] SET [tip] = ? WHERE [tip] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer upd.Close()

	result, err := upd.Exec(newTip.String(), oldTip.String())
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	if rowsAffected == 0 {
		return errors.Errorf(
			"Failed to update branch,  tip = %v", oldTip)
	}

	return nil
}

func updateBranch(tx *sql.Tx, snap *snapshot.Snapshot, name string) error {
	// TODO: Optimize this into using just one DB call(update) instead of deleting and insert
	err := deleteBranch(tx, snap)
	if err != nil {
		return err
	}

	return insertBranch(tx, snap, branch.NewRandomID(), name)
}

// deleteBranch deletes the branch that belongs to the given snapshot's parent
func deleteBranch(tx *sql.Tx, snap *snapshot.Snapshot) error {
	del, err := tx.Prepare(`
DELETE FROM [branch]
WHERE [tip] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer del.Close()

	_, err = del.Exec(
		snap.ParentID.String(),
	)

	if err != nil {
		return errors.New(err)
	}
	return nil
}

// insertBranch inserts a new branch using the given snapshot as the tip; branch name can be empty.
func insertBranch(tx *sql.Tx, snap *snapshot.Snapshot, branchID branch.ID, name string) error {
	ins, err := tx.Prepare(`
INSERT INTO [branch] ([id], [volumeset_id], [name], [tip])
VALUES (?, ?, ?, ?)
`)
	if err != nil {
		return errors.New(err)
	}
	defer ins.Close()

	var newName interface{}
	if !util.IsEmptyString(name) {
		newName = &name
	}

	_, err = ins.Exec(
		branchID.String(),
		snap.VolSetID.String(),
		newName,
		snap.ID.String(),
	)

	if err != nil {
		// TODO
		// Unfortunately this is the only way to validate the UNIQUE value constraint
		// from the sqlite.
		// This string should change if the scehma the table name or column name changes
		if err.Error() == "UNIQUE constraint failed: branch.volumeset_id, branch.name" {
			return errors.Errorf("Branch %s already exists", name)
		}

		return errors.New(err)
	}

	return nil
}

// NumVolumes ...
func (store *Sqlite3Storage) NumVolumes(snapid snapshot.ID) (int, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	numVolumes, err := snapGetNumVolumes(tx, snapid)
	if err != nil {
		return 0, err
	}

	return numVolumes, nil
}

// NumChildren ...
func (store *Sqlite3Storage) NumChildren(snapid snapshot.ID) (int, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	numChildren, err := snapGetNumChildren(tx, snapid)
	if err != nil {
		return 0, err
	}

	return numChildren, nil
}

// DeleteSnapshots ...
func (store *Sqlite3Storage) DeleteSnapshots(snaps []*snapshot.Snapshot, tip *snapshot.Snapshot) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	delBranch, err := tx.Prepare(`
DELETE FROM [branch]
WHERE [tip] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer delBranch.Close()

	delAttr, err := tx.Prepare(`
DELETE FROM [attributes]
WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer delAttr.Close()

	delSnap, err := tx.Prepare(`
DELETE FROM [snapshot]
WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}

	defer delSnap.Close()

	for _, s := range snaps {
		if s.ParentID == nil {
			err = deleteBush(tx, s.ID)
			if err != nil {
				return err
			}
		}

		for _, stmt := range []*sql.Stmt{
			delBranch,
			delAttr,
			delSnap,
		} {
			_, err = stmt.Exec(s.ID.String())
			if err != nil {
				return errors.New(err)
			}
		}
	}

	if tip == nil {
		return nil
	}

	// If the new tip is not parent of other snapshot(s), it becomes a new tip
	numChildren, err := snapGetNumChildren(tx, tip.ID)
	if err != nil {
		return err
	}

	if numChildren != 0 {
		return nil
	}

	// TODO: what happens to the existing branch after the top of the branch is deleted?
	return insertBranch(tx, tip, branch.NewRandomID(), "")
}

func snapGetNumChildren(tx *sql.Tx, snapid snapshot.ID) (int, error) {
	sel, err := tx.Prepare(`
SELECT count([id])
FROM [snapshot]
WHERE [parent_id] = ?
`)
	if err != nil {
		return 0, errors.New(err)
	}
	defer sel.Close()

	var cntStr string
	err = sel.QueryRow(snapid.String()).Scan(&cntStr)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, errors.New(err)
	}

	cnt, err := strconv.Atoi(cntStr)
	if err != nil {
		return 0, errors.Errorf("Failed to parse '%s': %v", cntStr, err)
	}

	return cnt, nil
}

func snapGetNumVolumes(tx *sql.Tx, snapid snapshot.ID) (int, error) {
	sel, err := tx.Prepare(`
SELECT count([id])
FROM [volume]
WHERE [parent_id] = ?
`)
	if err != nil {
		return 0, errors.New(err)
	}
	defer sel.Close()

	var cntStr string
	err = sel.QueryRow(snapid.String()).Scan(&cntStr)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, errors.New(err)
	}

	cnt, err := strconv.Atoi(cntStr)
	if err != nil {
		return 0, errors.Errorf("Failed to parse '%s': %v", cntStr, err)
	}

	return cnt, nil
}

// ImportBush ...
func (store *Sqlite3Storage) ImportBush(b *bush.Bush) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	ins, err := tx.Prepare(`
INSERT INTO [bush] ([volumeset_id], [id], [dsid])
VALUES (?, ?, ?)
`)
	if err != nil {
		return errors.New(err)
	}
	defer ins.Close()

	_, err = ins.Exec(
		b.VolSetID.String(),
		b.Root.String(),
		b.DataSrvID,
	)
	if err != nil {
		return errors.New(err)
	}

	return nil
}

// GetBush ...
func (store *Sqlite3Storage) GetBush(root snapshot.ID) (*bush.Bush, error) {
	var (
		vsid string
		dsid int
	)

	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	sel, err := tx.Prepare(`
SELECT [volumeset_id], [dsid]
FROM [bush]
WHERE [id] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer sel.Close()

	err = sel.QueryRow(root.String()).Scan(&vsid, &dsid)
	if err == sql.ErrNoRows {
		return nil, errors.Errorf("Bush does not exist from root snapshot ID: %s", root.String())
	}
	if err != nil {
		return nil, errors.New(err)
	}

	return &bush.Bush{
		VolSetID:  volumeset.NewID(vsid),
		DataSrvID: dsid,
		Root:      root,
	}, nil
}

// DeleteBush ...
func (store *Sqlite3Storage) DeleteBush(root snapshot.ID) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	return deleteBush(tx, root)
}

func deleteBush(tx *sql.Tx, root snapshot.ID) error {
	del, err := tx.Prepare(`
DELETE FROM [bush]
WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer del.Close()

	_, err = del.Exec(
		root.String(),
	)

	return err
}

// SetVolumeSetSize ...
func (store *Sqlite3Storage) SetVolumeSetSize(vsid volumeset.ID, size uint64) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	upd, err := tx.Prepare(`
UPDATE [volumeset] SET [size] = ? WHERE [id] = ?
`)
	if err != nil {
		return errors.New(err)
	}
	defer upd.Close()

	result, err := upd.Exec(size, vsid.String())
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	if rowsAffected == 0 {
		return errors.Errorf(
			"Failed to update volumeset(%v): no such volumeset", vsid)
	}

	return nil
}

// Implementation of data server manager

// Add ...
func (store *Sqlite3Storage) Add(srv *datasrvstore.Server) (*datasrvstore.Server, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	ins, err := tx.Prepare(`
INSERT INTO [dataserver] ([url], [current])
VALUES (?, ?)
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer ins.Close()

	_, err = ins.Exec(
		srv.URL,
		false,
	)
	if err != nil {
		return nil, errors.New(err)
	}

	sel, err := tx.Prepare(`
SELECT [id]
FROM [dataserver]
WHERE [url] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer sel.Close()

	var id int
	err = sel.QueryRow(srv.URL).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, errors.Errorf("Did not insert '%s' successfully", srv.URL)
	}
	if err != nil {
		return nil, errors.New(err)
	}

	return &datasrvstore.Server{
		URL:     srv.URL,
		Current: false,
		ID:      id,
	}, nil
}

// Get ...
func (store *Sqlite3Storage) Get(id int) (*datasrvstore.Server, error) {
	var (
		url     string
		current bool
	)
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	sel, err := tx.Prepare(`
SELECT [url], [current]
FROM [dataserver]
WHERE [id] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer sel.Close()

	err = sel.QueryRow(id).Scan(&url, &current)
	if err == sql.ErrNoRows {
		return nil, errors.Errorf("Did not find server %d", id)
	}
	if err != nil {
		return nil, errors.New(err)
	}

	return &datasrvstore.Server{
		ID:      id,
		URL:     url,
		Current: current,
	}, nil
}

// All ...
func (store *Sqlite3Storage) All() ([]*datasrvstore.Server, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	sel, err := tx.Prepare(`
SELECT [id], [url], [current]
FROM [dataserver]
ORDER BY [id] ASC
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer sel.Close()

	rows, err := sel.Query()
	if err == sql.ErrNoRows {
		return nil, errors.New("No dataserver in the table")
	}
	if err != nil {
		return nil, errors.New(err)
	}
	defer rows.Close()

	var srvs []*datasrvstore.Server
	for rows.Next() {
		var (
			id      int
			url     string
			current bool
		)
		err := rows.Scan(&id, &url, &current)
		if err != nil {
			return nil, errors.New(err)
		}
		srvs = append(srvs, &datasrvstore.Server{
			ID:      id,
			URL:     url,
			Current: current,
		})
	}

	return srvs, nil
}

// SetCurrent ...
func (store *Sqlite3Storage) SetCurrent(id int) error {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	upd1, err := tx.Prepare(`
	UPDATE [dataserver] SET [current] = ? WHERE [current] = ?
	`)
	if err != nil {
		return errors.New(err)
	}
	defer upd1.Close()

	upd2, err := tx.Prepare(`
	UPDATE [dataserver] SET [current] = ? WHERE [id] = ?
	`)
	if err != nil {
		return errors.New(err)
	}
	defer upd2.Close()

	result, err := upd1.Exec(false, true)
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	// Note: This can be caused by no current is set yet, ignore
	// if rowsAffected == 0 {
	// }

	result, err = upd2.Exec(true, id)
	if err != nil {
		return errors.New(err)
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return errors.New(err)
	}

	if rowsAffected == 0 {
		return errors.Errorf("Failed to set current data server(no record updated).")
	}

	if rowsAffected > 1 {
		return errors.Errorf("Failed to set current data server(multiple records updated).")
	}

	return nil
}

// GetCurrent ...
func (store *Sqlite3Storage) GetCurrent() (*datasrvstore.Server, error) {
	tx, err := store.db.Begin()
	defer func() {
		if err == nil {
			err = tx.Commit()
			if err != nil {
				err = errors.New(err)
			}
		} else {
			tx.Rollback()
		}
	}()

	sel, err := tx.Prepare(`
SELECT [id], [url]
FROM [dataserver]
WHERE [current] = ?
`)
	if err != nil {
		return nil, errors.New(err)
	}
	defer sel.Close()

	var (
		url string
		id  int
	)
	err = sel.QueryRow(true).Scan(&id, &url)
	if err == sql.ErrNoRows {
		return nil, errors.New("No current dataserver available")
	}
	if err != nil {
		return nil, errors.New(err)
	}

	return &datasrvstore.Server{
		ID:      id,
		URL:     url,
		Current: true,
	}, nil
}

// End of data server manager implementation
