package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	copier "github.com/wastore/lemur/copier/core"
	core "github.com/wastore/lemur/cmd/lhsm-plugin-az-core"
	"github.com/wastore/lemur/cmd/util"

	"github.com/pkg/errors"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/wastore/go-lustre"
	"github.com/wastore/lemur/go-lustre/fs"
	"github.com/wastore/lemur/go-lustre/status"

	"github.com/intel-hpdd/logging/debug"
	"github.com/wastore/lemur/dmplugin"
)

// Mover supports archiving/restoring data to/from Azure Storage
type Mover struct {
	name          string
	httpClient    *http.Client
	config        *archiveConfig
	copier        copier.Copier
	clientOptions *container.ClientOptions

	//*Channels to interact wtih SAS Manager
	getSAS          chan chan string
	forceSASRefresh chan time.Time
}

// AzMover returns a new *Mover
func AzMover(cfg *archiveConfig, archiveID uint32) *Mover {
	const MiB = int64(1024 * 1024)
	throughputBytesPerSec := int64(0)
	maxBlockLength := blockblob.MaxStageBlockBytes
	defaultConcurrency := 32
	cachelimit := 4 * 1024 * MiB // defaults 4 GiB

	if cfg.Bandwidth != 0 { // this value is in MB
		throughputBytesPerSec = int64(cfg.Bandwidth) * MiB
	}
	if cfg.CacheLimit != 0 { // This value is in GB
		cachelimit = int64(cfg.CacheLimit) * 1024 * MiB
	}

	copier := copier.NewCopier(throughputBytesPerSec, int64(maxBlockLength), cachelimit, defaultConcurrency)

	clientOptions := &container.ClientOptions{}
	clientOptions.Transport = &http.Client{
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
	}

	return &Mover{
		name:            fmt.Sprintf("az-%d", archiveID),
		copier:          copier,
		config:          cfg,
		clientOptions:   clientOptions,
		getSAS:          make(chan chan string),
		forceSASRefresh: make(chan time.Time),
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
	var containerName, path string

	u, err := url.ParseRequestURI(fileID)
	if err == nil {
		if u.Scheme != "az" {
			return "", "", errors.Errorf("invalid URL in file_id %s", fileID)
		}
		path = u.Path[1:]
		containerName = u.Host
	} else {
		path = m.destination(fileID)
		containerName = m.config.Container
	}
	return containerName, path, nil
}

// getSASToken will block till a valid sas is returned or timeout after a minute
func (m *Mover) getSASToken() (string, error) {
	ret := make(chan string)
	select {
	case m.getSAS <- ret:
		select {
		case sas := <-ret:
			return sas, nil
		case <-time.After(time.Minute):
			return "", errors.New("Failed to get SAS")
		}
	case <-time.After(time.Minute):
		return "", errors.New("Failed to get SAS. Refresh in progress")
	}
}

// returns true if we could successfully signal SAS manager to refresh creds in 1minute
func (m *Mover) refreshCredential(prevSASCtx time.Time) bool {
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

		expMultiplier := [4]int{4, 8, 16, 32}

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

		for try := 0; ; try++ { //loop till we've a valid SAS
			var nextTryInterval time.Duration
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
				m.config.setContainerURL()
				util.Log(pipeline.LogInfo, fmt.Sprint("Updated SAS at "+time.Now().String()))
				//Since refresh is successful, next refresh - after m.config.CredRefreshInterval
				util.Log(pipeline.LogInfo, fmt.Sprintf("Next refresh at %s", time.Now().Add(defaultRefreshInterval).String()))
				break
			}

			/*
			 * Failed to update SAS. We'll retry with exponential delay for upto a minute
			 * and after that we'll try every minute
			 *
			 * To not spam the log file, we'll only log first few retries and then once
			 * every hr.
			 */
			if try < 10 || try%60 == 0 {
				util.Log(pipeline.LogError, fmt.Sprintf(
					"Failed to update SAS.\nReason: %s, try: %d",
					err, (try+1)))
			}

			nextTryInterval = time.Minute
			if try < len(expMultiplier) {
				nextTryInterval = time.Duration(expMultiplier[try]) * time.Second
			}

			//retry after delay
			time.Sleep(nextTryInterval)
		}
	}
}

func (m *Mover) Archive(ctx context.Context, action dmplugin.Action) error {
	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID()))
	} else {
		util.Log(pipeline.LogInfo, fmt.Sprintf("%s id:%d archive %s", m.name, action.ID(), action.PrimaryPath()))
	}
	rate.Mark(1)
	start := time.Now()

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
	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("Path(s) on FS: %s", strings.Join(fnames, ", ")))
	}

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

	c := m.config.ContainerURL() + "?" + sas
	cURL, err := container.NewClientWithNoCredential(c, m.clientOptions)
	if err != nil {
		return errors.Wrap(err, "failed to get container client")
	}

	total, err := core.Archive(ctx, m.copier, core.ArchiveOptions{
		ContainerURL: cURL,
		ResourceSAS:  sas,
		MountRoot:    m.config.MountRoot,
		BlobName:     fileKey,
		SourcePath:   action.PrimaryPath(),
		BlockSize:    m.config.UploadPartSize,
		ExportPrefix: m.config.ExportPrefix,
		HTTPClient:   m.httpClient,
		OpStartTime:  opStartTime,
	})

	if util.ShouldRefreshCreds(err) {
		util.Log(pipeline.LogError, fmt.Sprintf("Refreshing creds for item %s", action.PrimaryPath()))
		m.refreshCredential(opStartTime)
	}

	if err != nil {
		return err
	}

	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d Archived %d bytes in %v from %s to %s/%s", m.name, action.ID(), total,
			time.Since(start),
			action.PrimaryPath(),
			m.config.Container, fileKey))
	} else {
		util.Log(pipeline.LogInfo, fmt.Sprintf("%s id:%d Archived %d bytes in %v from %s", m.name, action.ID(), total,
			time.Since(start),
			action.PrimaryPath()))
	}

	u, _ := url.Parse(m.config.ContainerURL())
	u.Scheme = "az"

	action.SetUUID(fileID)
	action.SetURL(u.String())
	action.SetActualLength(total)
	return nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(ctx context.Context, action dmplugin.Action) error {
	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID()))
	} else {
		util.Log(pipeline.LogInfo, fmt.Sprintf("%s id:%d restore %s", m.name, action.ID(), action.PrimaryPath()))
	}
	rate.Mark(1)

	start := time.Now()
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

	c := m.config.ContainerURL() + "?" + sas
	cURL, err := container.NewClientWithNoCredential(c, m.clientOptions)
	if err != nil {
		return errors.Wrap(err, "failed to get container client")
	}

	contentLen, err := core.Restore(ctx, m.copier, core.RestoreOptions{
		ContainerURL:    cURL,
		BlobName:        srcObj,
		DestinationPath: action.WritePath(),
		BlockSize:       m.config.UploadPartSize,
		ExportPrefix:    m.config.ExportPrefix,
		HTTPClient:      m.httpClient,
	})

	if util.ShouldRefreshCreds(err) {
		util.Log(pipeline.LogError, fmt.Sprintf("Refreshing creds for item %s", action.PrimaryPath()))
		m.refreshCredential(opStartTime)
	}

	if err != nil {
		return err
	}

	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("%s id:%d Restored %d bytes in %v from %s to %s", m.name, action.ID(), contentLen,
			time.Since(start),
			srcObj,
			action.PrimaryPath()))
	} else {
		util.Log(pipeline.LogInfo, fmt.Sprintf("%s id:%d Restored %d bytes in %v to %s", m.name, action.ID(), contentLen,
			time.Since(start),
			action.PrimaryPath()))

	}

	action.SetActualLength(contentLen)
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(ctx context.Context, action dmplugin.Action) error {
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

	c := m.config.ContainerURL() + "?" + sas
	cURL, err := container.NewClientWithNoCredential(c, m.clientOptions)
	if err != nil {
		return errors.Wrap(err, "failed to get container client")
	}

	err = core.Remove(ctx, core.RemoveOptions{
		ContainerURL: cURL,
		BlobName:     srcObj,
		ExportPrefix: m.config.ExportPrefix,
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
