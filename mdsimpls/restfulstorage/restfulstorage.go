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

// Package restfulstorage is a package for remote access to metadata via a RESTful HTTP interface.
package restfulstorage

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/dp/sync"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/protocols"
	"github.com/ClusterHQ/fli/rest"
	"github.com/ClusterHQ/fli/vh/cauthn"
)

type (
	// HubAddress is an encapsulation for an addess of a remote MDS.
	// TODO: Remove this struct to directly access URL in MetadataStorage
	HubAddress struct {
		RootResource *url.URL
	}

	// MetadataStorage is an implementation of MetadataStorage interface
	// that acts as a proxy to another MetadataStorage implementation
	// that is accessible via RESTful HTTP API.
	MetadataStorage struct {
		httpClient *protocols.Client
		hubAddress HubAddress
		vhut       *cauthn.VHUT
	}
)

var (
	_ metastore.Syncable = &MetadataStorage{}
	_ sync.BlobAccepter  = &MetadataStorage{}
	_ sync.BlobSpewer    = &MetadataStorage{}
)

// Create creates a new object with MetadataStorage interface that acts as a proxy to another
// MetadataStorage implementation that is accessible via RESTful HTTP API.
// Takes an initialized http.Client, leaving connection details out of scope.
// Probably needs an authorization token for performing various operations,
// to be added later.
func Create(httpClient *protocols.Client, url *url.URL, vhut *cauthn.VHUT) (*MetadataStorage, error) {
	return &MetadataStorage{
		httpClient: httpClient,
		hubAddress: HubAddress{RootResource: url},
		vhut:       vhut,
	}, nil
}

// newAuthHTTPRequest Generates a http.Request with authorization token in the http header
func (rs *MetadataStorage) newAuthHTTPRequest(method, urlStr string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}

	if rs.vhut != nil {
		// TODO: Log missing auth token in header
		if err := rs.vhut.UpdateRequest(req); err != nil {
			return req, err
		}
	}

	corrID := protocols.GenerateCorrelationID()
	protocols.SetCorrelationID(req, corrID)
	return req, nil
}

// ImportVolumeSet ...
func (rs *MetadataStorage) ImportVolumeSet(vs *volumeset.VolumeSet) error {
	payload, err := json.Marshal(protocols.ReqVolSet{vs})
	if err != nil {
		return err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathVolumeSet)
	if err != nil {
		return err
	}

	req, err := rs.newAuthHTTPRequest("PUT", u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusCreated {
		return nil
	}
	if httpResp.StatusCode == http.StatusConflict {
		return &metastore.ErrVolumeSetAlreadyExists{}
	}
	return responseToError(httpResp)
}

// GetVolumeSets ...
func (rs *MetadataStorage) GetVolumeSets(q volumeset.Query) ([]*volumeset.VolumeSet, error) {
	payload, err := json.Marshal(
		protocols.ReqGetVolumeSets{
			Query: q,
		})
	if err != nil {
		return nil, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathVolumeSets)
	if err != nil {
		return nil, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	var respGetVolSets protocols.RespGetVolumeSets
	err = resp.GetResult(&respGetVolSets)
	if err != nil {
		return nil, err
	}

	return respGetVolSets.VolumeSets, nil
}

// ImportBranch ...
func (rs *MetadataStorage) ImportBranch(branchID branch.ID, branchName string, snapshots ...*snapshot.Snapshot) error {
	payload, err := json.Marshal(protocols.ReqImportBranch{branchID, branchName, snapshots})
	if err != nil {
		return err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathImportBranch)
	if err != nil {
		return err
	}

	req, err := rs.newAuthHTTPRequest("PUT", u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusCreated {
		return nil
	}
	if httpResp.StatusCode == http.StatusConflict {
		return &metastore.ErrSnapshotImportMismatch{}
	}
	return responseToError(httpResp)
}

// ForkBranch ...
func (rs *MetadataStorage) ForkBranch(branchName string, snapshots ...*snapshot.Snapshot) error {
	payload, err := json.Marshal(protocols.ReqForkBranch{branchName, snapshots})
	if err != nil {
		return err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathForkBranch)
	if err != nil {
		return err
	}

	req, err := rs.newAuthHTTPRequest("PUT", u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusCreated {
		return nil
	}
	if httpResp.StatusCode == http.StatusConflict {
		return &metastore.ErrSnapshotImportMismatch{}
	}
	return responseToError(httpResp)
}

// ExtendBranch ...
func (rs *MetadataStorage) ExtendBranch(snapshots ...*snapshot.Snapshot) error {
	payload, err := json.Marshal(protocols.ReqExtendBranch{snapshots})
	if err != nil {
		return err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathExtendBranch)
	if err != nil {
		return err
	}

	req, err := rs.newAuthHTTPRequest("PUT", u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusCreated {
		return nil
	}
	if httpResp.StatusCode == http.StatusConflict {
		return &metastore.ErrSnapshotImportMismatch{}
	}
	return responseToError(httpResp)
}

// GetTip ...
func (rs *MetadataStorage) GetTip(vsid volumeset.ID, branch string) (*snapshot.Snapshot, error) {
	payload, err := json.Marshal(protocols.ReqBranch{vsid, branch})
	if err != nil {
		return nil, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathTip)
	if err != nil {
		return nil, err
	}

	req, err := rs.newAuthHTTPRequest("GET", u.String(),
		bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusNotFound {
		// XXX Can / should we distinguish between ErrBranchNotFound and ErrVolumeSetNotFound?
		return nil, &metastore.ErrBranchNotFound{}
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	var respGetTip protocols.RespGetTip
	err = resp.GetResult(&respGetTip)
	if err != nil {
		return nil, err
	}

	return respGetTip.Snapshot, nil
}

// GetBranches ...
func (rs *MetadataStorage) GetBranches(q branch.Query) ([]*branch.Branch, error) {
	r := protocols.ReqGetBranches{Query: q}
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathBranches)
	if err != nil {
		return nil, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	var respGetBranches protocols.RespGetBranches
	err = resp.GetResult(&respGetBranches)
	if err != nil {
		return nil, err
	}

	return respGetBranches.Branches, nil
}

// responseToError converts an unexpected http.Response to an error object
// that contains some useful information from the response.
func responseToError(resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Errorf("Response status %s, failed to read message: %s", resp.Status, err)
	}
	if len(body) > 0 {
		return errors.Errorf("Response status %s, message: %s", resp.Status, body)
	}
	return errors.Errorf("Response status %s", resp.Status)
}

// OfferBlobDiff issues an HTTP request to a dataplane server for blob diff upload negotiation.
func (rs *MetadataStorage) OfferBlobDiff(vsid volumeset.ID, targetID snapshot.ID,
	baseCandidateIDs []snapshot.ID) (*snapshot.ID, string, string, error) {
	payload, err := json.Marshal(protocols.ReqSyncBlob{vsid, targetID, baseCandidateIDs})
	if err != nil {
		return nil, "", "", err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathOfferBlob)
	if err != nil {
		return nil, "", "", err
	}

	req, err := rs.newAuthHTTPRequest("GET", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, "", "", err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusOK {
		var r protocols.RespSyncBlob
		err = json.NewDecoder(httpResp.Body).Decode(&r)
		if err != nil {
			return nil, "", "", err
		}

		token, base := r.Token, r.BaseSnapshotID
		if err != nil {
			return nil, "", "", err
		}

		if token == "" {
			return nil, "", "", &metastore.ErrAlreadyHaveBlob{}
		}

		if base.IsNilID() {
			return nil, token, r.DataServerPublicURL, nil
		}

		return &base, token, r.DataServerPublicURL, nil
	}

	return nil, "", "", responseToError(httpResp)
}

// RequestBlobDiff issues an HTTP request to a dataplane server for blob diff download negotiation.
func (rs *MetadataStorage) RequestBlobDiff(vsid volumeset.ID, targetID snapshot.ID,
	baseCandidateIDs []snapshot.ID) (*snapshot.ID, string, string, error) {
	payload, err := json.Marshal(protocols.ReqSyncBlob{vsid, targetID, baseCandidateIDs})
	if err != nil {
		return nil, "", "", err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathRequestBlob)
	if err != nil {
		return nil, "", "", err
	}

	req, err := rs.newAuthHTTPRequest("GET", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, "", "", err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusOK {
		var r protocols.RespSyncBlob
		err = json.NewDecoder(httpResp.Body).Decode(&r)
		if err != nil {
			return nil, "", "", err
		}

		token, base := r.Token, r.BaseSnapshotID
		return &base, token, r.DataServerPublicURL, nil
	}

	return nil, "", "", responseToError(httpResp)
}

// GetSnapshots is a pass-through to Adapter's GetSnapshots.
// Note: caller is responsible for actually filtering output based on query.
func (rs *MetadataStorage) GetSnapshots(q snapshot.Query) ([]*snapshot.Snapshot, error) {
	r := protocols.ReqGetSnapshots{q}
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathSnapshots)
	if err != nil {
		return nil, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	var respGetSnapshots protocols.RespGetSnapshots
	err = resp.GetResult(&respGetSnapshots)
	if err != nil {
		return nil, err
	}

	return respGetSnapshots.Snapshots, nil
}

// UpdateVolumeSet ...
func (rs *MetadataStorage) UpdateVolumeSet(vsCur, vsInit *volumeset.VolumeSet) (metastore.VSMetaConflict, error) {
	payload, err := json.Marshal(protocols.ReqUpdateVolumeSet{VolSetCur: vsCur, VolSetInit: vsInit})
	noConflict := metastore.VSMetaConflict{}

	if err != nil {
		return noConflict, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathUpdateVolumeSet)
	if err != nil {
		return noConflict, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return noConflict, err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return noConflict, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return noConflict, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return noConflict, err
	}

	var respGetVSConfl protocols.RespUpdateVolumeSet
	err = resp.GetResult(&respGetVSConfl)
	if err != nil {
		return noConflict, err
	}

	return respGetVSConfl.VSMetaConfl, nil
}

// UpdateSnapshot ...
func (rs *MetadataStorage) UpdateSnapshot(snCur, snInit *snapshot.Snapshot) (metastore.SnapMetaConflict, error) {
	payload, err := json.Marshal(protocols.ReqUpdateSnapshot{SnapCur: snCur, SnapInit: snInit})
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathUpdateSnapshot)
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return metastore.SnapMetaConflict{}, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}

	var respGetSnapConfl protocols.RespUpdateSnapshot
	err = resp.GetResult(&respGetSnapConfl)
	if err != nil {
		return metastore.SnapMetaConflict{}, err
	}

	return respGetSnapConfl.SnapMetaConfl, nil
}

// UpdateSnapshots implements metastore interface
func (rs *MetadataStorage) UpdateSnapshots(snaps []*metastore.SnapshotPair) ([]metastore.SnapMetaConflict, error) {
	payload, err := json.Marshal(protocols.ReqUpdateSnapshots{Snaps: snaps})
	if err != nil {
		return nil, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathUpdateSnapshots)
	if err != nil {
		return nil, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	var respGetSnapConfls protocols.RespUpdateSnapshots
	err = resp.GetResult(&respGetSnapConfls)
	if err != nil {
		return nil, err
	}

	return respGetSnapConfls.SnapMetaConfls, nil
}

// GetSnapshotIDs ...
func (rs *MetadataStorage) GetSnapshotIDs(vsid volumeset.ID) ([]snapshot.ID, error) {
	r := protocols.ReqGetSnapshotIDs{VolSetID: vsid}
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	u, err := rs.hubAddress.RootResource.Parse(protocols.HTTPPathSnapshotIDs)
	if err != nil {
		return nil, err
	}

	req, err := rs.newAuthHTTPRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	httpResp, err := rs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, responseToError(httpResp)
	}

	var resp rest.Response
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	var respGetSnapshotIDs protocols.RespGetSnapshotIDs
	err = resp.GetResult(&respGetSnapshotIDs)
	if err != nil {
		return nil, err
	}

	return respGetSnapshotIDs.IDs, nil
}
