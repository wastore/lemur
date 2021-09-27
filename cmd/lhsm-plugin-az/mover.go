package main

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
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

// Mover supports archiving/restoring data to/from Azure Storage
type Mover struct {
	name            string
	cred            azblob.Credential
	httpClient      *http.Client
	config          *archiveConfig
	forceSASRefresh chan chan bool
}

// AzMover returns a new *Mover
func AzMover(cfg *archiveConfig, creds azblob.Credential, archiveID uint32) *Mover {
	return &Mover{
		name:   fmt.Sprintf("az-%d", archiveID),
		cred:   creds,
		config: cfg,
		httpClient: &http.Client{
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
		forceSASRefresh: make(chan chan bool),
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
	util.InitJobLogger(pipeline.LogDebug)
	util.Log(pipeline.LogDebug, fmt.Sprintf("%s started", m.name))
	go m.refreshCredentialInt()
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

func (m *Mover) getSASToken() string {
	m.config.configLock.Lock()
	defer m.config.configLock.Unlock()
	return m.config.AzStorageSAS
}

func(m *Mover) refreshCredential() {
	ret := make (chan bool)
	m.forceSASRefresh <- ret
	<-ret
}

func (m *Mover) refreshCredentialInt() {
	const sigAzure = "sig="
	defaultRefreshInterval, _ := time.ParseDuration(m.config.CredRefreshInterval)
	nextRefreshInterval := defaultRefreshInterval
	retryDelay := 4 * time.Second // 4 seconds
	try := 1 // Number of retries to get the SAS. Starts at 1.
	var respChan chan bool //Respond if someone is waiting for refresh

	for {
		select {
		case respChan = <-m.forceSASRefresh:
		case <-time.After(nextRefreshInterval):
			sas, err := util.GetKVSecret(m.config.AzStorageKVURL, m.config.AzStorageKVSecretName)

			if err == nil && !strings.Contains(sas, sigAzure) {
				err = fmt.Errorf("Invalid SAS returned")
			}

			if err != nil {

				util.Log(pipeline.LogError, fmt.Sprintf(
					"Failed to update SAS.\nReason: %s, try: %d",
					err, try))

				/*
				 * Failed to update SAS. We'll retry with exponential delay.
				 */
				nextRefreshInterval = time.Duration(math.Pow(2, float64(try))-1) * retryDelay
				if nextRefreshInterval >= time.Duration(1 * time.Minute) {
					nextRefreshInterval = 1 * time.Minute
				}
				try++
				util.Log(pipeline.LogError, fmt.Sprintf(
					"Next refresh at %s",
					time.Now().Add(nextRefreshInterval).String()))
				continue
			}

			//Refresh successful, next refresh - after m.config.CredRefreshInterval
			nextRefreshInterval = defaultRefreshInterval
			try = 1
			util.Log(pipeline.LogInfo, fmt.Sprint("Updated SAS at "+time.Now().String()))
			util.Log(pipeline.LogInfo, fmt.Sprintf("Next refresh at %s", time.Now().Add(nextRefreshInterval).String()))

			m.config.configLock.Lock()
			m.config.AzStorageSAS = sas
			m.config.configLock.Unlock()

			if respChan != nil {
				respChan <- true
				respChan = nil
			}
		}
	}
}

// Archive fulfills an HSM Archive request
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

	total, err := core.Archive(core.ArchiveOptions{
		AccountName:     m.config.AzStorageAccount,
		BlobEndpointURL: m.config.BlobEndpointURL,
		DFSEndpointURL:  m.config.DFSEndpointURL,
		ContainerName:   m.config.Container,
		ResourceSAS:     m.getSASToken(),
		MountRoot:       m.config.MountRoot,
		BlobName:        fileKey,
		Credential:      m.cred,
		SourcePath:      action.PrimaryPath(),
		Parallelism:     uint16(m.config.NumThreads),
		BlockSize:       m.config.UploadPartSize,
		Pacer:           pacer,
		ExportPrefix:    m.config.ExportPrefix,
		HNSEnabled:      m.config.HNSEnabled,
		HTTPClient:      m.httpClient,
	})

	if err != nil {
		return errors.Wrap(err, "upload failed")
	}

	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.Response().StatusCode == 403 {
			m.refreshCredential()
		}
	}

	util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d Archived %d bytes in %v from %s to %s/%s", m.name, action.ID(), total,
		time.Since(start),
		action.PrimaryPath(),
		m.config.Container, fileKey))

	u := url.URL{
		Scheme: "az",
		Host:   fmt.Sprintf("%s.blob.core.windows.net/%s", m.config.AzStorageAccount, m.config.Container),
		Path:   fileKey,
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
	container, srcObj, err := m.fileIDtoContainerPath(action.UUID())
	if err != nil {
		return errors.Wrap(err, "fileIDtoContainerPath failed")
	}

	contentLen, err := core.Restore(core.RestoreOptions{
		AccountName:     m.config.AzStorageAccount,
		BlobEndpointURL: m.config.BlobEndpointURL,
		ContainerName:   container,
		ResourceSAS:     m.getSASToken(),
		BlobName:        srcObj,
		Credential:      m.cred,
		DestinationPath: action.WritePath(),
		Parallelism:     uint16(m.config.NumThreads),
		BlockSize:       m.config.UploadPartSize,
		ExportPrefix:    m.config.ExportPrefix,
		Pacer:           pacer,
		HTTPClient:      m.httpClient,
	})

	if err != nil {
		return errors.Errorf("az.Download() of %s failed: %s", srcObj, err)
	}

	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.Response().StatusCode == 403 {
			m.refreshCredential()
		}
	}

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

	container, srcObj, err := m.fileIDtoContainerPath(string(action.UUID()))
	if err != nil {
		return errors.Wrap(err, "fileIDtoContainerPath failed")
	}

	err = core.Remove(core.RemoveOptions{
		AccountName:     m.config.AzStorageAccount,
		BlobEndpointURL: m.config.BlobEndpointURL,
		ContainerName:   container,
		ResourceSAS:     m.getSASToken(),
		BlobName:        srcObj,
		ExportPrefix:    m.config.ExportPrefix,
		Credential:      m.cred,
	})

	if err != nil {
		return errors.Wrap(err, "delete object failed")
	}

	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.Response().StatusCode == 403 {
			m.refreshCredential()
		}
	}
	
	return nil
}
