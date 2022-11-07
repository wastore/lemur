package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	core "github.com/wastore/lemur/cmd/lhsm-plugin-az-core"
	"github.com/wastore/lemur/cmd/util"

	"github.com/pkg/errors"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/go-lustre"
	"github.com/wastore/go-lustre/fs"
	"github.com/wastore/go-lustre/status"

	"github.com/intel-hpdd/logging/debug"
	"github.com/wastore/lemur/dmplugin"
)

type cancelMap struct {
	m map[string]context.CancelFunc
	l sync.Mutex
}

func (c *cancelMap) add(key string, v context.CancelFunc) {
	c.l.Lock()
	defer c.l.Unlock()
	c.m[key] = v
}

func (c *cancelMap) delete(key string) {
	c.l.Lock()
	defer c.l.Unlock()
	delete(c.m, key)
}

func (c *cancelMap) cancelFunc(key string) (context.CancelFunc, bool) {
	c.l.Lock()
	defer c.l.Unlock()
	f, ok := c.m[key]
	return f, ok
}

// Mover supports archiving/restoring data to/from Azure Storage
type Mover struct {
	name                string
	cred                azblob.Credential
	httpClient          *http.Client
	config              *archiveConfig
	actions             cancelMap //Actions in progress

	//*Channels to interact wtih SAS Manager
	getSAS              chan chan string
	forceSASRefresh     chan time.Time
}

// AzMover returns a new *Mover
func AzMover(cfg *archiveConfig, creds azblob.Credential, archiveID uint32) *Mover {
	return &Mover{
		name:                fmt.Sprintf("az-%d", archiveID),
		cred:                creds,
		config:              cfg,
		httpClient: 		 &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:           0, // No limit
				MaxIdleConnsPerHost:    cfg.NumThreads,
				IdleConnTimeout:        180 * time.Second,
				TLSHandshakeTimeout:    10 * time.Second,
				ExpectContinueTimeout:  1 * time.Second,
				DisableKeepAlives:      false,
				DisableCompression:     true, 
				MaxResponseHeaderBytes: 0,
			},
		},
		getSAS:             make(chan chan string),
		forceSASRefresh:    make(chan time.Time),
		actions:            cancelMap{m: make(map[string]context.CancelFunc)},
	}
}

func (m *Mover) destination(id string) string {
	if m.config.Prefix != "" {
		return path.Join(m.config.Prefix, id)
	} else {
		return id
	}
}

// Start signals the mover to begin any asynchronous processing (e.g. stats)
func (m *Mover) Start() {
	util.Log(pipeline.LogDebug, fmt.Sprintf("%s started", m.name))
	go m.SASManager()
	debug.Printf("%s started", m.name)
}

func (m *Mover) fileIDtoContainerPath(fileID string) (string, string, error) {
	var container, path string

	u, err := url.ParseRequestURI(fileID)
	if err == nil {
		if u.Scheme != "az" {
			return "", "", errors.Errorf("invalid URL in file_id %s", fileID)
		}
		path = u.Path[1:]
		container = u.Host
	} else {
		path = m.destination(fileID)
		container = m.config.Container
	}
	return container, path, nil
}

// getSASToken will block till a valid sas is returned or timeout after a minute
func (m *Mover) getSASToken() (string, error) {
	ret := make(chan string)
	select {
	case m.getSAS <- ret:
		select {
		case sas := <- ret:
			return sas, nil
		case <-time.After(time.Minute):
			return "", errors.New("Failed to get SAS")
		}
	case <-time.After(time.Minute):
		return "", errors.New("Failed to get SAS. Refresh in progress")
	}
}

// returns true if we could successfully signal SAS manager to refresh creds in 1minute
func(m *Mover) refreshCredential(prevSASCtx time.Time) bool {
	select {
	case m.forceSASRefresh <- prevSASCtx: //this will block until we've requested for a refresh
		return true
	case <-time.After(time.Minute):
		return false
	}
}

/*
 * SASManager() 
 * - Returns valid SAS on received channel when Archive/Restore/Return operations ask for it
 * - Updates SAS when operations request for it (i.e. when they fail with 403)
 * - Updates SAS every `CredRefreshInterval`
 * Also, checkAzAccess() would put a valid SAS before Mover is started, and hence SASManager
 * is always seeded with a valid SAS
 */
 func (m *Mover) SASManager() {
	defaultRefreshInterval, _ := time.ParseDuration(m.config.CredRefreshInterval)

	for {
		retryDelay := 4 * time.Second
		try := 1 // Number of retries to get the SAS. Starts at 1.
		var nextTryInterval time.Duration

		select {
		case <-time.After(defaultRefreshInterval): // we always try to refresh
		case reqCtx := <-m.forceSASRefresh:
			if reqCtx.Before(m.config.SASContext) {
				//Nothing to be done, we've already updated sas.
				continue
			} // else we refresh sas
		case retChan := <-m.getSAS: //lowest priority is to return SAS
			retChan <- m.config.AzStorageSAS
			continue
		}

		for { //loop till we've a valid SAS
			sas, err := util.GetKVSecret(m.config.AzStorageKVURL, m.config.AzStorageKVSecretName)
			if err == nil {
				if ok, reason := util.IsSASValid(sas); !ok {
					err = errors.New("Invalid SAS returned. " + reason)
				}
			}

			if err == nil {
				//we've a valid SAS
				m.config.AzStorageSAS = sas
				m.config.SASContext = time.Now()
				util.Log(pipeline.LogInfo, fmt.Sprint("Updated SAS at "+time.Now().String()))
				util.Log(pipeline.LogInfo, fmt.Sprintf("Next refresh at %s", time.Now().Add(nextTryInterval).String()))
				break
			}

			/*
			 * Failed to update SAS. We'll retry with exponential delay.
			 */
			util.Log(pipeline.LogError, fmt.Sprintf(
				"Failed to update SAS.\nReason: %s, try: %d",
				err, try))
			nextTryInterval = time.Duration(math.Pow(2, float64(try))-1) * retryDelay
			if nextTryInterval >= time.Duration(1 * time.Minute) {
				nextTryInterval = 1 * time.Minute
			}
			try++

			//retry after delay
			time.Sleep(nextTryInterval)
		}
	}	
}

func (m *Mover) Archive(action dmplugin.Action) error {
	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID()))
	rate.Mark(1)
	start := time.Now()

	var pacer util.Pacer
	/* start pacer if required */
	if m.config.Bandwidth != 0 {
		util.Log(pipeline.LogDebug, fmt.Sprintf("Starting pacer with bandwidth %d\n", m.config.Bandwidth))
		pacer = util.NewTokenBucketPacer(int64(m.config.Bandwidth*1024*1024), int64(0))
		defer pacer.Close()
	}

	// translate the fid into an actual path first
	fidStr := strings.TrimPrefix(action.PrimaryPath(), ".lustre/fid/")
	fid, err := lustre.ParseFid(fidStr)
	if err != nil {
		return errors.Wrap(err, "failed to parse fid")
	}
	rootDir, err := fs.MountRoot(m.config.MountRoot)
	if err != nil {
		return errors.Wrap(err, "failed to find root dir")
	}
	fnames, err := status.FidPathnames(rootDir, fid)
	if err != nil {
		return errors.Wrap(err, "failed to get pathname")
	}
	util.Log(pipeline.LogDebug, fmt.Sprintf("Path(s) on FS: %s", strings.Join(fnames, ", ")))

	if len(fnames) > 1 {
		util.Log(pipeline.LogDebug, "WARNING: multiple paths returned, using first")
	}
	fileID := fnames[0]
	fileKey := m.destination(fileID)

	opStartTime := time.Now()
	sas, err := m.getSASToken()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.actions.add(action.PrimaryPath(),cancel)

	total, err := core.Archive(ctx, core.ArchiveOptions{
		ContainerURL:  m.config.ContainerURL(),
		ResourceSAS:   sas,
		MountRoot:     m.config.MountRoot,
		BlobName:      fileKey,
		Credential:    m.cred,
		SourcePath:    action.PrimaryPath(),
		Parallelism:   uint16(m.config.NumThreads),
		BlockSize:     m.config.UploadPartSize,
		Pacer:         pacer,
		ExportPrefix:  m.config.ExportPrefix,
		HTTPClient:    m.httpClient,
		OpStartTime:   opStartTime,
	})

	if util.ShouldRefreshCreds(err) {
		util.Log(pipeline.LogError, fmt.Sprintf("Refreshing creds for item %s", action.PrimaryPath()))
		m.refreshCredential(opStartTime)
	}

	if err != nil {
		return err
	}

	m.actions.delete(action.PrimaryPath())

	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d Archived %d bytes in %v from %s to %s/%s", m.name, action.ID(), total,
		time.Since(start),
		action.PrimaryPath(),
		m.config.Container, fileKey))

	u := url.URL{
		Scheme: "az",
		Host:   m.config.ContainerURL().Host,
		Path:   m.config.ContainerURL().Path,
	}

	action.SetUUID(fileID)
	action.SetURL(u.String())
	action.SetActualLength(total)
	return nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action dmplugin.Action) error {
	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID()))
	rate.Mark(1)

	var pacer util.Pacer

	start := time.Now()
	if m.config.Bandwidth != 0 {
		util.Log(pipeline.LogDebug, fmt.Sprintf("Starting pacer with bandwith %d MBPS\n", m.config.Bandwidth))
		pacer = util.NewTokenBucketPacer(int64(m.config.Bandwidth*1024*1024), int64(0))
		defer pacer.Close()
	}
	if action.UUID() == "" {
		return errors.Errorf("Missing file_id on action %d", action.ID())
	}
	_, srcObj, err := m.fileIDtoContainerPath(action.UUID())
	if err != nil {
		return errors.Wrap(err, "fileIDtoContainerPath failed")
	}

	opStartTime := time.Now()
	sas, err := m.getSASToken()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.actions.add(action.PrimaryPath(), cancel)

	contentLen, err := core.Restore(ctx, core.RestoreOptions{
		ResourceSAS:     sas,
		BlobName:        srcObj,
		Credential:      m.cred,
		DestinationPath: action.WritePath(),
		Parallelism:     uint16(m.config.NumThreads),
		BlockSize:       m.config.UploadPartSize,
		ExportPrefix:    m.config.ExportPrefix,
		Pacer:           pacer,
		HTTPClient:      m.httpClient,
	})

	if util.ShouldRefreshCreds(err) {
		util.Log(pipeline.LogError, fmt.Sprintf("Refreshing creds for item %s", action.PrimaryPath()))
		m.refreshCredential(opStartTime)
	}

	if err != nil {
		return err
	}

	m.actions.delete(action.PrimaryPath())

	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d Restored %d bytes in %v from %s to %s", m.name, action.ID(), contentLen,
		time.Since(start),
		srcObj,
		action.PrimaryPath()))

	action.SetActualLength(contentLen)
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error { 
	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d remove %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID()))
	rate.Mark(1)
	if action.UUID() == "" {
		return errors.New("Missing file_id")
	}

	_, srcObj, err := m.fileIDtoContainerPath(string(action.UUID()))
	if err != nil {
		return errors.Wrap(err, "fileIDtoContainerPath failed")
	}

	opStartTime := time.Now()
	sas, err := m.getSASToken()
	if err != nil {
		return err
	}

	err = core.Remove(core.RemoveOptions{
		ContainerURL:   m.config.ContainerURL(),
		ResourceSAS:   sas,
		BlobName:      srcObj,
		ExportPrefix:  m.config.ExportPrefix,
		Credential:    m.cred,
	})

	if util.ShouldRefreshCreds(err) {
		util.Log(pipeline.LogError, fmt.Sprintf("Refreshing creds for item %s", action.PrimaryPath()))
		m.refreshCredential(opStartTime)
	}

	if err != nil {
		return err
	}

	return nil
}


func (m *Mover) Cancel(action dmplugin.Action) error {
	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d Cancel %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID()))
	if cancel, ok := m.actions.cancelFunc(action.PrimaryPath()); ok {
		cancel()
		return nil
	}
	return errors.New("Could not find action with specified UUID")
}