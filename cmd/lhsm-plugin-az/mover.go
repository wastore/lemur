package main

import (
	"fmt"
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
	name                string
	cred                azblob.Credential
	nextCredRefreshTime time.Time
	config              *archiveConfig
}

// AzMover returns a new *Mover
func AzMover(cfg *archiveConfig, creds azblob.Credential, archiveID uint32) *Mover {
	return &Mover{
		name:                fmt.Sprintf("az-%d", archiveID),
		cred:                creds,
		nextCredRefreshTime: time.Now().Add(-1 * time.Second), // So that first action updates SAS
		config:              cfg,
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

func (m *Mover) refreshCredential() {
	const sigAzure = "sig="
	var err error
	if m.nextCredRefreshTime.After(time.Now()) {
		return
	}

	m.cred = azblob.NewAnonymousCredential()
	sas, err := util.GetKVSecret(m.config.AzStorageKVName, m.config.AzStorageKVSecretName)

	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Failed to update SAS. Falling back to previous SAS.\n%s", err))
		return
	}

	if !strings.Contains(sas, sigAzure) {
		util.Log(pipeline.LogError, fmt.Sprintf("Failed to update SAS, invalid SAS returned. Falling back to previous SAS."))
		return
	}

	//Refresh successful, next refresh - at least 24 hrs later
	util.Log(pipeline.LogInfo, fmt.Sprint("Updated SAS at "+time.Now().String()))
	m.nextCredRefreshTime = time.Now().Add(24 * time.Hour)
	m.config.AzStorageSAS = sas
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

	m.refreshCredential()

	total, err := core.Archive(core.ArchiveOptions{
		AccountName:   m.config.AzStorageAccount,
		ContainerName: m.config.Container,
		ResourceSAS:   m.config.AzStorageSAS,
		MountRoot:     m.config.MountRoot,
		BlobName:      fileKey,
		Credential:    m.cred,
		SourcePath:    action.PrimaryPath(),
		Parallelism:   uint16(m.config.NumThreads),
		BlockSize:     m.config.UploadPartSize,
		Pacer:         pacer,
		ExportPrefix:  m.config.ExportPrefix,
		HNSEnabled:    m.config.HNSEnabled,
	})

	if err != nil {
		return errors.Wrap(err, "upload failed")
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

	m.refreshCredential()

	contentLen, err := core.Restore(core.RestoreOptions{
		AccountName:     m.config.AzStorageAccount,
		ContainerName:   container,
		ResourceSAS:     m.config.AzStorageSAS,
		BlobName:        srcObj,
		Credential:      m.cred,
		DestinationPath: action.WritePath(),
		Parallelism:     uint16(m.config.NumThreads),
		BlockSize:       m.config.UploadPartSize,
		ExportPrefix:    m.config.ExportPrefix,
		Pacer:           pacer,
	})

	if err != nil {
		return errors.Errorf("az.Download() of %s failed: %s", srcObj, err)
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

	m.refreshCredential()

	err = core.Remove(core.RemoveOptions{
		AccountName:   m.config.AzStorageAccount,
		ContainerName: container,
		ResourceSAS:   m.config.AzStorageSAS,
		BlobName:      srcObj,
		ExportPrefix:  m.config.ExportPrefix,
		Credential:    m.cred,
	})

	if err != nil {
		return errors.Wrap(err, "delete object failed")
	}

	return nil
}
