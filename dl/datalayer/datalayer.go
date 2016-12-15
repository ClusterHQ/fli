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

package datalayer

import (
	"encoding/binary"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/ClusterHQ/fli/dl/encdec"
	dlbin "github.com/ClusterHQ/fli/dl/encdec/binary"
	dlgob "github.com/ClusterHQ/fli/dl/encdec/gob"
	"github.com/ClusterHQ/fli/dl/executor"
	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/ClusterHQ/fli/dl/hash/adler32"
	"github.com/ClusterHQ/fli/dl/hash/md5"
	"github.com/ClusterHQ/fli/dl/hash/noop"
	dlsha "github.com/ClusterHQ/fli/dl/hash/sha256"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/protocols"
	"github.com/ClusterHQ/fli/protocols/transferhdr"
	"github.com/ClusterHQ/fli/securefilepath"
)

// Code shared by different datalayer implementations

type (
	// BlobDifferFactory defines the blob differ factory interface
	BlobDifferFactory interface {
		New(path1, path2 string, hf dlhash.Factory, exErrCh chan<- error,
			cancel <-chan bool, wg *sync.WaitGroup) <-chan record.Record
	}

	// MountType defines mount mode when a new volume is created
	MountType bool

	// Storage defines common interface for all data storage back ends
	Storage interface {
		// Returns the storage version
		Version() string

		// BlobDiffer returns the associated blob differ of the storage
		BlobDiffer() BlobDifferFactory

		// MountBlob mounts a blob.
		MountBlob(blob.ID) (string, error)

		// EmptyBlobID returns a blob id where a new volume can be created from
		EmptyBlobID(volumeset.ID) (blob.ID, error)

		// CreateVolume creates a volume.
		CreateVolume(volumeset.ID, blob.ID, MountType) (volume.ID, securefilepath.SecureFilePath, error)

		// DestroyVolume destroys the volume
		DestroyVolume(volumeset.ID, volume.ID) error

		// CreateSnapshot takes a snapshot off a volume
		CreateSnapshot(volumeset.ID, snapshot.ID, volume.ID) (blob.ID, error)

		// DestroySnapshot destroys an existing snapshot
		DestroySnapshot(blob.ID) error

		// DestroyVolumeSet forcefully destroys an existing volumeset(all data belongs to the volume set will
		// be permanently removed)
		DestroyVolumeSet(volumeset.ID) error

		// SnapshotExists checks if the blob
		SnapshotExists(blob.ID) (bool, error)

		// GetSnapshotSpace returns space usage statistics for a snapshot.
		GetSnapshotSpace(blob.ID) (SnapshotSpace, error)

		// GetTotalSpace returns space usage statistics for the whole storage.
		GetTotalSpace() (DiskSpace, error)

		// GetVolumesetSpace returns space usage statistics for a volume set.
		GetVolumesetSpace(vsid volumeset.ID) (DiskSpace, error)

		// Unmount unmount a previously mounted path.
		Unmount(path string) error
	}

	// SnapshotSpace is a structure that describes various aspects
	// of the space usage by a single snapshot.
	// The sizes are in bytes.
	SnapshotSpace struct {
		// LogicalSize is a visible size of data contained in the snapshot.
		LogicalSize uint64
		// DeltaFromPrevious is an on-disk size of new data in the snapshot
		// comparing to the previous snapshot.
		DeltaFromPrevious uint64
	}

	// DiskSpace is a structure that describes various aspects
	// of the on-disk space usage by various data structures such as
	// volumes, volume sets and a whole storage.
	// The sizes are in bytes.
	DiskSpace struct {
		// Used is an on-disk size of data contained in the data structure.
		Used uint64

		// Available is a size that's available on disk for the use by data
		// in the data structure.
		// A sum of Used and Available equals a capacity of the data structure.
		Available uint64
	}
)

const (
	// NoAutoMount is the false value for MountType.
	// This flag is used to tell storage layer when creating a volume, set it to no automatic
	// mount mode, which means after use the volume is unmounted, and will not be mounted
	// after a reboot.
	NoAutoMount MountType = false

	// AutoMount is the true value for MountType
	// This flag is used to tell storage layer when creating a volume, set it to automatic
	// mount mode, which means after use the volume remains mounted, and will be mounted
	// after a reboot.
	AutoMount MountType = true

	// DifferChannelSize ...
	DifferChannelSize = 20
)

// UploadBlobDiff ...
func UploadBlobDiff(
	s Storage,
	encdec encdec.Factory,
	hf dlhash.Factory,
	vsid volumeset.ID,
	base blob.ID,
	targetBlobID blob.ID,
	token string,
	dspuburl string,
) error {
	var (
		baseBlobID blob.ID
		err        error
	)

	if base.IsNilID() {
		baseBlobID, err = s.EmptyBlobID(vsid)
		if err != nil {
			return err
		}
	} else {
		baseBlobID = base
	}

	reqBody, reqWriter := io.Pipe()

	url := makeUploadURL(dspuburl, token)
	req, err := http.NewRequest("PUT", url, reqBody)
	if err != nil {
		return err
	}

	correlationID := protocols.GenerateCorrelationID()
	protocols.SetCorrelationID(req, correlationID)

	wg := &sync.WaitGroup{}
	errc := make(chan error, 1)
	wg.Add(1)
	// Starts the http connection, client.Do() will block until the pipe is closed, that's why it is in a go
	// routine. The pipe is written to by differ.
	go func(errc chan<- error, wg *sync.WaitGroup) {
		defer wg.Done()

		req.Header.Set("Content-Length", "0")
		client := protocols.GetClient()
		resp, err := client.Do(req)
		if err != nil {
			errc <- err
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			errc <- errors.Errorf("HTTP request for upload failed with status %d", resp.StatusCode)
		}
	}(errc, wg)

	// If http failed to start, quit
	select {
	case err = <-errc:
		return err
	default:
	}

	err = SendDiff(s, baseBlobID, targetBlobID, encdec, hf, reqWriter)
	if err != nil {
		reqWriter.CloseWithError(err)
	} else {
		reqWriter.Close()
	}

	wg.Wait()
	select {
	case err = <-errc:
		return err
	default:
		return nil
	}
}

// DownloadBlobDiff receives records from an HTTP server, apply them to the local backing storage.
func DownloadBlobDiff(
	s Storage,
	encdec encdec.Factory,
	vsid volumeset.ID,
	ssid snapshot.ID,
	base blob.ID,
	token string,
	e executor.Executor,
	hf dlhash.Factory,
	dspuburl string,
) (blob.ID, uint64, uint64, error) {
	var (
		baseBlobID blob.ID
		err        error
	)
	if base.IsNilID() {
		baseBlobID, err = s.EmptyBlobID(vsid)
		if err != nil {
			return blob.NilID(), 0, 0, errors.New(err)
		}
	} else {
		baseBlobID = base
	}

	// Can't do much if we don't have the blob of the parent(don't know where to apply the new records to)
	if exist, _ := s.SnapshotExists(baseBlobID); exist == false {
		return blob.NilID(), 0, 0, nil
	}

	var (
		vid     volume.ID
		mntPath securefilepath.SecureFilePath
	)
	vid, mntPath, err = s.CreateVolume(vsid, baseBlobID, NoAutoMount)
	if err != nil {
		return blob.NilID(), 0, 0, errors.New(err)
	}

	dlURL := makeDownloadURL(dspuburl, token)
	req, err := http.NewRequest("GET", dlURL, nil)
	if err != nil {
		return blob.NilID(), 0, 0, errors.New(err)
	}

	correlationID := protocols.GenerateCorrelationID()
	protocols.SetCorrelationID(req, correlationID)
	client := protocols.GetClient()
	resp, err := client.Do(req)
	if err != nil {
		return blob.NilID(), 0, 0, errors.New(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return blob.NilID(), 0, 0, errors.Errorf("HTTP request for download failed with status %d", resp.StatusCode)
	}

	err = ReceiveDiff(resp.Body, mntPath, e)
	if err != nil {
		return blob.NilID(), 0, 0, err
	}

	// Take a snapshot
	blobid, err := s.CreateSnapshot(vsid, ssid, vid)
	if err != nil {
		return blob.NilID(), 0, 0, err
	}

	// Clean up when finished, destroy the volume created for the upload.
	// Error is logged, no need to return to caller.
	// Note: This still won't remove the volume set because how ZFS works. See zfs.go for details.
	err = s.DestroyVolume(vsid, vid)
	if err != nil {
		log.Printf("Delete volume error %v after download volume set %v volume %v.", err, vsid, vid)
	}

	snapSize, err := s.GetSnapshotSpace(blobid)
	if err != nil {
		return blob.NilID(), 0, 0, err
	}

	vsSize, err := s.GetVolumesetSpace(vsid)
	if err != nil {
		return blob.NilID(), 0, 0, err
	}

	return blobid, snapSize.LogicalSize, vsSize.Used, nil
}

// ApplyDiff receives syscall records and apply to the given mount point.
// In order to achieve max disk throughput, this function supports executing records in parallel.
// Multiple worker threads are created, each thread works with a dedicated channel.
// Records are dispatched into different channels based on the status of the channels(how busy, for example).
// One extra channel is created for all worker threads to report back completion of each record it processes.
// TODO: Limit total number of download/upload in parallel at the data server level in order to prevent too many files
// open error.
// TODO: Use only one global workers object?
type (
	result struct {
		r   record.Record
		err error
	}

	// waiter stores information about each object including how many are ongoing and all waiting records
	waiter struct {
		// ongoing is a counter of how many AsyncExec records are in flight(waiting in a worker's queue or
		// being executed
		ongoing int

		// recordWaiting stores waiting DelayExec records for an object
		recordWaiting []record.Record
	}

	workers struct {
		numWorkers  int
		channelSize int
		mntPath     securefilepath.SecureFilePath
		executor    executor.Executor

		queues  []chan record.Record
		results chan result
		waiters map[string]*waiter

		mutex *sync.Mutex
		wg    *sync.WaitGroup
		hf    dlhash.Factory
	}
)

// dispatch dispatches a record to a worker's queue.
// TOTO: Be able to inject different queue selection algorithm
func (wrks *workers) dispatch(r record.Record) {
	wrks.mutex.Lock()
	defer wrks.mutex.Unlock()

	// Find the shortest queue
	var idx int
	for i := 1; i < wrks.numWorkers; i++ {
		if len(wrks.queues[i]) < len(wrks.queues[idx]) {
			idx = i
		}
	}

	// Dispatch the record to the queue
	wrks.queues[idx] <- r

	// Add or update record's waiter info
	if w, ok := wrks.waiters[r.Key()]; ok {
		w.ongoing++
	} else {
		wrks.waiters[r.Key()] = &waiter{
			ongoing: 1,
		}
	}
}

// startWorkers creates and start all worker threads
// Note: Needs to allocate the results channel to be at least big enough to hold all issued
// requests in order to avoid deadlock between the dispatch thread and workers.
func startWorkers(numWorkers int, channelSize int, mntPath securefilepath.SecureFilePath,
	executor executor.Executor, hf dlhash.Factory) *workers {
	wrks := workers{
		numWorkers: numWorkers,
		mutex:      &sync.Mutex{},
		queues:     make([]chan record.Record, numWorkers, numWorkers),
		results:    make(chan result, channelSize*numWorkers),
		wg:         &sync.WaitGroup{},
		waiters:    make(map[string]*waiter),
		mntPath:    mntPath,
		executor:   executor,
		hf:         hf,
	}

	wrks.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		wrks.queues[i] = make(chan record.Record, channelSize)
		go worker(mntPath, executor, hf, wrks.queues[i], wrks.results, wrks.wg)
	}

	return &wrks
}

// recordCompleted post processing after a record is executed.
func (wrks *workers) recordCompleted(r record.Record) error {
	var execDelayedRecords bool

	wrks.mutex.Lock()
	w := wrks.waiters[r.Key()]
	w.ongoing--
	if w.ongoing == 0 {
		delete(wrks.waiters, r.Key())
		execDelayedRecords = true
	}
	wrks.mutex.Unlock()

	// Executes all delayed records if there are no more ongoing requests which have the same record key
	if execDelayedRecords {
		for _, r := range w.recordWaiting {
			err := wrks.executor.Execute(wrks.mntPath.Path(), []record.Record{r})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ReceiveDiff reads records from the source, send them to the applier
func ReceiveDiff(src io.Reader, mntPath securefilepath.SecureFilePath, e executor.Executor) error {
	var (
		err error
		// Note: Define here, had trouble with err reflected outside for loop when using recs, err := ...
		recs []record.Record
	)

	hdr, err := readTransferHdr(src)
	if err != nil {
		return err
	}

	if transferhdr.CurVer != hdr.Ver {
		return errors.Errorf("Version %v is not supported, current version is %v.", hdr.Ver, transferhdr.CurVer)
	}

	encdec, err := EncDecFactory(hdr.EncDec)
	if err != nil {
		return err
	}

	hf, err := HashFactory(hdr.Hash)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	records := make(chan record.Record, 128)
	errc := applyDiff(mntPath, e, hf, records, wg)
	d := encdec.NewDecoder(src)

receiveRecords:
	for {
		select {
		case err = <-errc:
			break receiveRecords
		default:
			recs, err = d.Decode()
			if err == io.EOF {
				err = nil
				break receiveRecords
			}
			if err != nil {
				break receiveRecords
			}

			for _, r := range recs {
				records <- r
			}
		}
	}

	close(records)
	wg.Wait()

	if err != nil {
		return err
	}

	if !e.EOT() {
		return errors.New("Missing EOT")
	}
	return nil
}

// applyDiff receives records from a channel and execute each record.
// An assumption is made here is records are generated in the following fashion:
// Meta records(create, renae, unlink, etc) for a certain object
// Data records(pwrite)
// Special record(setmtime)
// Meta records of another object
// Data records
// Special record
// ...
// Meta records are executed one at a time.
// Data records can be executed in parallel in order to gain max disk I/O throughput.
// Special record like setmtime() can only be executed after both meta and data records are executed.
// each record has an identification key to associate it with an object(for example a file or a path).
// Execution mode is determined by a record's ExecType().
// Sync: Executed by the thread that reads the record from the record channel(only one) and executed synchronously.
// Async: Executed by worker threads. There are multiple worker threads.
// Delay: Executed when all ongoing records for the same object are all completed. Upon receiving these records, they
//        are either executed as syncronously if there are no ongoing Sync records of the same key, or it is cached
//        memory for later execution. The delayed execution is triggered by the completion of a Async record.
func applyDiff(mntPath securefilepath.SecureFilePath, e executor.Executor, hf dlhash.Factory,
	records <-chan record.Record, wg *sync.WaitGroup) <-chan error {

	errc := make(chan error, 1)
	wg.Add(1)

	go func(mntPath securefilepath.SecureFilePath, e executor.Executor, hf dlhash.Factory,
		records <-chan record.Record, wg *sync.WaitGroup, errc chan<- error) {
		defer wg.Done()

		// Note: Considerations on how many worker threads vs channel size.
		// Number of workers limits how many I/O requests can be sent at the same time to the I/O system.
		// Channel size * number of worker dictates when will the caller be blocked.
		wrks := startWorkers(128, 100, mntPath, e, hf)

		var err error
		// Loop forever and only quit when no more records from the channel.
		// In case of error, report back to caller and caller will close the channel to let us know when to
		// stop by closing the channel.
		// Only report error once becaus ethe channel has only one slot.
		// Once an error happens, incoming records are hrown away without been executed.
	recordAndResults:
		for {
			select {
			// Records channel
			case r, more := <-records:
				if !more {
					break recordAndResults
				}

				if err != nil {
					// Already saw an error, throw away the record
					continue
				}

				switch r.ExecType() {
				case record.SyncExec:
					err = e.Execute(mntPath.Path(), []record.Record{r})
					if err != nil {
						errc <- err
						continue
					}

				case record.AsyncExec:
					wrks.dispatch(r)

				case record.DelayExec:
					delay := false
					wrks.mutex.Lock()
					if w, ok := wrks.waiters[r.Key()]; ok {
						w.recordWaiting = append(w.recordWaiting, r)
						delay = true
					}
					wrks.mutex.Unlock()
					if !delay {
						err = e.Execute(mntPath.Path(), []record.Record{r})
						if err != nil {
							errc <- err
							continue
						}
					}
				}

			// Results channel
			case result := <-wrks.results:
				if err != nil {
					// Error already happened, ignore results
					continue
				}

				if result.err != nil {
					err = result.err
					errc <- err
					continue
				}

				err = wrks.recordCompleted(result.r)
				if err != nil {
					errc <- err
					continue
				}
			}
		}

		// Stop all worker
		for i := 0; i < wrks.numWorkers; i++ {
			close(wrks.queues[i])
		}
		wrks.wg.Wait()
		close(wrks.results)

		if err == nil {
			// Left over results in the channel waiting to be processed
			for result := range wrks.results {
				if result.err != nil {
					errc <- err
					break
				}

				err = wrks.recordCompleted(result.r)
				if err != nil {
					errc <- err
					break
				}
			}
		}
	}(mntPath, e, hf, records, wg, errc)

	return errc
}

// worker gets request from the records channel, executes the record, and reports status back to caller through the
// results channel
func worker(mntPath securefilepath.SecureFilePath, executor executor.Executor, hf dlhash.Factory,
	records <-chan record.Record, results chan<- result, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		r, more := <-records
		if !more {
			// All done, no more records to receive
			return
		}

		match, err := record.VerifyChksum(r, hf)
		if err != nil {
			results <- result{
				r:   r,
				err: err,
			}
			continue
		}

		if !match {
			results <- result{
				r:   r,
				err: errors.New("Checksum mistmatch"),
			}
			continue
		}

		err = executor.Execute(mntPath.Path(), []record.Record{r})
		results <- result{
			r:   r,
			err: err,
		}
	}
}

func endInSlash(path string) string {
	if path[len(path)-1:] != "/" {
		path = path + "/"
	}
	return path
}

func makeUploadURL(serverURL string, tok string) string {
	serverURL = endInSlash(serverURL)
	return serverURL + protocols.HTTPReqUploadBlob + "?" + protocols.HTTPFieldToken + "=" + url.QueryEscape(tok)
}

func makeDownloadURL(serverURL string, tok string) string {
	serverURL = endInSlash(serverURL)
	return serverURL + protocols.HTTPReqDownloadBlob + "?" + protocols.HTTPFieldToken + "=" + url.QueryEscape(tok)
}

// Differ - sender work flow:
// Differ generates records, send to a channel, sender consumes the records.
// Sender doesn't stop until the record channel is closed.
// In case of sender error, sender notifies differ through the cancellation channel. Differ quits if a cancel request
// is received.
// In case of differ error, differ closes the record channel to let sender know and sender will stop.
// Differ and sender errors are reported back to the caller through the external error channel.

// SendDiff sends records to a server, records are read from a channel
func SendDiff(s Storage, baseBlobID blob.ID, targetBlobID blob.ID, encdec encdec.Factory, hf dlhash.Factory,
	target io.Writer) error {
	exist, err := s.SnapshotExists(baseBlobID)
	if exist == false || err != nil {
		return errors.Errorf("Base blob %v not found", baseBlobID)
	}

	exist, err = s.SnapshotExists(targetBlobID)
	if exist == false || err != nil {
		err = errors.Errorf("Target blob %v not found", targetBlobID)
		return err
	}

	basePath, err := s.MountBlob(baseBlobID)
	if err != nil {
		return err
	}
	defer s.Unmount(baseBlobID.String())

	targetPath, err := s.MountBlob(targetBlobID)
	if err != nil {
		return err
	}
	defer s.Unmount(targetBlobID.String())

	err = writeTransferHdr(
		target,
		&transferhdr.Hdr{
			Ver:    transferhdr.CurVer,
			Hash:   hf.Type(),
			EncDec: encdec.Type(),
		})
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	errc := make(chan error, 1)
	cancelCh := make(chan bool, 1)

	wg.Add(1)
	records := s.BlobDiffer().New(basePath, targetPath, hf, errc, cancelCh, wg)
	err = SendRecords(encdec, records, target, cancelCh)
	wg.Wait()

	select {
	case errDiffer := <-errc:
		return errDiffer
	default:
		return err
	}
}

// SendRecords reads records from the channel, encode and send them to the target(for example an http link).
// Stops only after received all records. In case of error, will notify differ to stop through the cancel channel.
func SendRecords(encdec encdec.Factory, records <-chan record.Record, target io.Writer, cancelCh chan<- bool) error {
	var err error

	e := encdec.NewEncoder(target)
	for {
		r, more := <-records
		if !more {
			break
		}

		if err != nil {
			// Throw away records, until told to stop.
			continue
		}

		err = e.Encode([]record.Record{r})
		if err != nil {
			cancelCh <- true
			continue
		}
	}

	return err
}

// EncDecFactory returns a enc/dec factory instance based on the type given
func EncDecFactory(t encdec.Type) (encdec.Factory, error) {
	switch t {
	case encdec.Binary:
		return dlbin.Factory{}, nil
	case encdec.Gob:
		return dlgob.Factory{}, nil
	default:
		return nil, errors.Errorf("Invalid encoder/decoder type %v", t)
	}
}

// HashFactory returns a hash factory instance based on the type given
func HashFactory(t dlhash.Type) (dlhash.Factory, error) {
	switch t {
	case dlhash.NoOp:
		return noop.Factory{}, nil
	case dlhash.Adler32:
		return adler32.Factory{}, nil
	case dlhash.MD5:
		return md5.Factory{}, nil
	case dlhash.SHA256:
		return dlsha.Factory{}, nil
	default:
		return nil, errors.Errorf("Invalid hash type %v", t)
	}
}

func writeTransferHdr(target io.Writer, hdr *transferhdr.Hdr) error {
	err := binary.Write(target, binary.LittleEndian, uint64(hdr.Ver))
	if err != nil {
		return err
	}

	err = binary.Write(target, binary.LittleEndian, uint64(hdr.EncDec))
	if err != nil {
		return err
	}

	err = binary.Write(target, binary.LittleEndian, uint64(hdr.Hash))
	if err != nil {
		return err
	}

	return nil
}

func readTransferHdr(src io.Reader) (*transferhdr.Hdr, error) {
	var (
		hdr transferhdr.Hdr
		n   uint64
	)

	err := binary.Read(src, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	hdr.Ver = int(n)

	err = binary.Read(src, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	hdr.EncDec = encdec.Type(n)

	err = binary.Read(src, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	hdr.Hash = dlhash.Type(n)

	return &hdr, nil
}
