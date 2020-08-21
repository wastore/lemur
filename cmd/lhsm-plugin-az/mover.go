package main

import (
	"fmt"
	core "github.com/wastore/lemur/cmd/lhsm-plugin-az-core"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/go-lustre"
	"github.com/wastore/go-lustre/fs"
	"github.com/wastore/go-lustre/status"

	"github.com/intel-hpdd/logging/debug"
	"github.com/wastore/lemur/dmplugin"
)

// Mover supports archiving/restoring data to/from Azure Storage
type Mover struct {
	name   string
	cred   *azblob.SharedKeyCredential
	config *archiveConfig
}

// AzMover returns a new *Mover
func AzMover(cfg *archiveConfig, creds *azblob.SharedKeyCredential, archiveID uint32) *Mover {
	return &Mover{
		name:   fmt.Sprintf("az-%d", archiveID),
		cred:   creds,
		config: cfg,
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
	debug.Printf("Parsed %s -> %s / %s", fileID, container, path)
	return container, path, nil
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action dmplugin.Action) error {
	debug.Printf("%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)
	start := time.Now()

	// translate the fid into an actual path first
	fidStr := strings.TrimPrefix(action.PrimaryPath(), ".lustre/fid/")
	fid, err := lustre.ParseFid(fidStr)
	if err != nil {
		return errors.Wrap(err, "failed to parse fid")
	}
	rootDir, err := fs.MountRoot("/mnt/lhsmd/agent")
	if err != nil {
		return errors.Wrap(err, "failed to find root dir")
	}
	fnames, err := status.FidPathnames(rootDir, fid)
	if err != nil {
		return errors.Wrap(err, "failed to get pathname")
	}
	debug.Printf("Path(s) on FS: %s", strings.Join(fnames, ", "))

	if len(fnames) > 1 {
		debug.Printf("WARNING: multiple paths returned, using first")
	}
	fileID := fnames[0]
	fileKey := m.destination(fileID)

	total, err := core.Archive(core.ArchiveOptions{
		AccountName:   m.config.AzStorageAccount,
		ContainerName: m.config.Container,
		BlobName:      fileKey,
		Credential:    m.cred,
		SourcePath:    action.PrimaryPath(),
		Parallelism:   uint16(m.config.NumThreads),
		BlockSize:     m.config.UploadPartSize,
	})

	if err != nil {
		return errors.Wrap(err, "upload failed")
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s/%s", m.name, action.ID(), total,
		time.Since(start),
		action.PrimaryPath(),
		m.config.Container, fileKey)

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
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)

	start := time.Now()
	if action.UUID() == "" {
		return errors.Errorf("Missing file_id on action %d", action.ID())
	}
	container, srcObj, err := m.fileIDtoContainerPath(action.UUID())
	if err != nil {
		return errors.Wrap(err, "fileIDtoContainerPath failed")
	}

	contentLen, err := core.Restore(core.RestoreOptions{
		AccountName: m.config.AzStorageAccount,
		ContainerName: container,
		BlobName: srcObj,
		Credential: m.cred,
		DestinationPath: action.WritePath(),
		Parallelism: uint16(m.config.NumThreads),
		BlockSize: m.config.UploadPartSize,
	})

	if err != nil {
		return errors.Errorf("az.Download() of %s failed: %s", srcObj, err)
	}

	debug.Printf("%s id:%d Restored %d bytes in %v from %s to %s", m.name, action.ID(), contentLen,
		time.Since(start),
		srcObj,
		action.PrimaryPath())

	action.SetActualLength(contentLen)
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error {
	debug.Printf("%s id:%d remove %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)
	if action.UUID() == "" {
		return errors.New("Missing file_id")
	}

	container, srcObj, err := m.fileIDtoContainerPath(string(action.UUID()))
	if err != nil {
		return errors.Wrap(err, "fileIDtoContainerPath failed")
	}

	err = core.Remove(core.RemoveOptions{
		AccountName: m.config.AzStorageAccount,
		ContainerName: container,
		BlobName: srcObj,
		Credential: m.cred,
	})

	if err != nil {
		return errors.Wrap(err, "delete object failed")
	}

	return nil
}
