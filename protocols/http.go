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

package protocols

import (
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

type (
	// ReqVolSet ..
	ReqVolSet struct {
		VolSet *volumeset.VolumeSet
	}

	// ReqUpdateVolumeSet ...
	ReqUpdateVolumeSet struct {
		VolSetInit *volumeset.VolumeSet `json:"volset_init"`
		VolSetCur  *volumeset.VolumeSet `json:"volset_cur"`
	}

	// RespVolSet ..
	RespVolSet struct {
		VolSet *volumeset.VolumeSet
		Err    error
	}

	// ReqVolSetID ...
	ReqVolSetID struct {
		ID volumeset.ID `json:"id"`
	}

	// ReqSnapshot ...
	ReqSnapshot struct {
		Snapshot *snapshot.Snapshot
	}

	// ReqUpdateSnapshot ...
	ReqUpdateSnapshot struct {
		SnapInit *snapshot.Snapshot `json:"snap_init"`
		SnapCur  *snapshot.Snapshot `json:"snap_cur"`
	}

	// ReqUpdateSnapshots ...
	ReqUpdateSnapshots struct {
		Snaps []*metastore.SnapshotPair `json:"snap_pairs"`
	}

	// ReqSnapshotID ...
	ReqSnapshotID struct {
		ID snapshot.ID `json:"id"`
	}

	// ReqBlobID ...
	ReqBlobID struct {
		ID blob.ID
	}

	// ReqBranch ..
	ReqBranch struct {
		VolSetID volumeset.ID
		Branch   string
	}

	// ReqImportBranch ..
	ReqImportBranch struct {
		ID        branch.ID
		Branch    string
		Snapshots []*snapshot.Snapshot
	}

	// ReqForkBranch ..
	ReqForkBranch struct {
		Branch    string
		Snapshots []*snapshot.Snapshot
	}

	// ReqExtendBranch ..
	ReqExtendBranch struct {
		Snapshots []*snapshot.Snapshot
	}

	// ReqUploadToken ...
	ReqUploadToken struct {
		VolumeSetID volumeset.ID
		BaseBlobID  blob.ID
	}

	// ReqDownloadToken ...
	ReqDownloadToken struct {
		VolumeSetID  volumeset.ID
		BaseBlobID   blob.ID
		TargetBlobID blob.ID
	}

	// RespToken is the response from DS to DP when DP request either a
	// upload or download token.
	RespToken struct {
		Token               string
		DataServerPublicURL string
	}

	// ReqUploadTokenStatus ..
	ReqUploadTokenStatus struct {
		Token  string
		BlobID blob.ID
		Size   uint64
		Status string
	}

	// ReqDownloadTokenStatus ..
	ReqDownloadTokenStatus struct {
		Token  string
		Status string
	}

	// ReqSyncBlob ..
	ReqSyncBlob struct {
		VolSetID         volumeset.ID
		TargetID         snapshot.ID
		BaseCandidateIDs []snapshot.ID
	}

	// RespSyncBlob ..
	RespSyncBlob struct {
		Token               string
		DataServerPublicURL string
		BaseSnapshotID      snapshot.ID
	}

	// ReqGetVolumeSets ...
	ReqGetVolumeSets struct {
		volumeset.Query
	}

	// RespGetVolumeSets ..
	RespGetVolumeSets struct {
		Total      int                    `json:"total_volumesets"`
		VolumeSets []*volumeset.VolumeSet `json:"volumesets"`
	}

	// ReqGetSnapshots ..
	ReqGetSnapshots struct {
		snapshot.Query
	}

	// RespGetSnapshots ..
	RespGetSnapshots struct {
		Total     int                  `json:"total_snapshots"`
		Snapshots []*snapshot.Snapshot `json:"snapshots"`
	}

	// ReqGetBranches ..
	ReqGetBranches struct {
		branch.Query
	}

	// RespGetBranches ..
	RespGetBranches struct {
		Total    int              `json:"total_branches"`
		Branches []*branch.Branch `json:"branches"`
	}

	// ReqGetSnapshotIDs ..
	ReqGetSnapshotIDs struct {
		VolSetID volumeset.ID
	}

	// RespGetSnapshotIDs ..
	RespGetSnapshotIDs struct {
		IDs []snapshot.ID `json:"ids"`
	}

	// RespGetTip ...
	RespGetTip struct {
		Snapshot *snapshot.Snapshot
	}

	// ReqURL ..
	ReqURL struct {
		URL string
	}

	// ReqDataServerStats returns stats of storage or a volume set
	// If volume set id is given, it returns the stats of the volume set; otherwise, returns stats of the storage
	ReqDataServerStats struct {
		VolumeSetID volumeset.ID
	}

	// RespDataServerStats is the struct for reporting data server stats
	RespDataServerStats struct {
		DiskSize uint64
		Used     uint64
	}

	// RespUpdateVolumeSet ..
	RespUpdateVolumeSet struct {
		VSMetaConfl metastore.VSMetaConflict `json:"vs_meta_confl"`
	}

	// RespUpdateSnapshot ..
	RespUpdateSnapshot struct {
		SnapMetaConfl metastore.SnapMetaConflict `json:"snap_meta_confl"`
	}

	// RespUpdateSnapshots ..
	RespUpdateSnapshots struct {
		SnapMetaConfls []metastore.SnapMetaConflict `json:"snap_meta_confls"`
	}
)

const (
	// HTTPPathVersion indicates the version
	HTTPPathVersion = "v1"
	// Requests from client to data plane

	// HTTPPathVolumeSet get/create/delete volume set
	HTTPPathVolumeSet = "volumeset"
	// HTTPPathVolumeSets get all volume sets
	HTTPPathVolumeSets = "volumesets"
	// HTTPPathSnapshots get all snapshots
	HTTPPathSnapshots = "snapshots"
	// HTTPPathBranches get all branches
	HTTPPathBranches = "branches"
	// HTTPPathSnapshotIDs get all snapshot IDs
	HTTPPathSnapshotIDs = "snapshotids"
	// HTTPPathBlob ...
	HTTPPathBlob = "blob"
	// HTTPPathTip get tip of a branch
	HTTPPathTip = "tip"
	// HTTPPathImportBranch Import snapshots by importing a new branch
	HTTPPathImportBranch = "importbranch"
	// HTTPPathForkBranch Import snapshots by forking a new branch
	HTTPPathForkBranch = "forkbranch"
	// HTTPPathExtendBranch Import snapshots by extending an existing branch
	HTTPPathExtendBranch = "extendbranch"
	// HTTPPathOfferBlob offer to push a blob
	HTTPPathOfferBlob = "upload/blob"
	// HTTPPathRequestBlob request to pull a blob
	HTTPPathRequestBlob = "download/blob"
	// HTTPPathSnapshotByBranch obtains snapshots of a branch
	HTTPPathSnapshotByBranch = "snapshotsbybranch"
	// HTTPPathStats get stats
	HTTPPathStats = "stats"
	// HTTPPathNewDataServer put data server url
	HTTPPathNewDataServer = "dataserver"

	// Requests from client to data server

	// HTTPReqUploadBlob upload a blob with token
	HTTPReqUploadBlob = "upload"
	// HTTPReqDownloadBlob download a blob with token
	HTTPReqDownloadBlob = "download"
	// HTTPFieldToken token field use as a parameter in blob upload/download requests
	HTTPFieldToken = "token"

	// Requests from data plane to data server

	// HTTPPathUploadToken get upload token
	HTTPPathUploadToken = "upload/token"
	// HTTPPathDownloadToken get download token
	HTTPPathDownloadToken = "download/token"

	// Requests from data server to data plane(upload/download status)

	// HTTPPathUploadStatus upload status
	HTTPPathUploadStatus = "upload/status"
	// HTTPPathDownloadStatus download status
	HTTPPathDownloadStatus = "download/status"

	// HTTPPathUpdateVolumeSet ...
	HTTPPathUpdateVolumeSet = "update/volumeset"

	// HTTPPathUpdateSnapshot ...
	HTTPPathUpdateSnapshot = "update/snapshot"

	// HTTPPathUpdateSnapshots ...
	HTTPPathUpdateSnapshots = "update/snapshots"
)

// ErrHTTPXferTimeout transfer timed out error
var ErrHTTPXferTimeout = "HTTP Transfer Timeout"
