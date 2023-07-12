package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	copier "github.com/nakulkar-msft/copier/core"
	"github.com/wastore/lemur/cmd/util"
)

type ArchiveOptions struct {
	ContainerURL *container.Client
	ResourceSAS  string
	MountRoot    string
	BlobName     string
	SourcePath   string
	BlockSize    int64
	ExportPrefix string
	HTTPClient   *http.Client
	OpStartTime  time.Time
}

func (a *ArchiveOptions) getUploadOptions(filepath string) (*blockblob.UploadFileOptions, error) {
	meta := make(map[string]*string)

	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}

	owner := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	permissions := uint32(fileInfo.Mode().Perm())
	if fileInfo.Mode()&os.ModeSticky != 0 {
		permissions |= syscall.S_ISVTX
	}
	group := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)
	modTime := fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")

	permissionsString := fmt.Sprintf("%04o", permissions)

	meta["permissions"] = &permissionsString
	meta["modtime"] = &modTime
	meta["owner"] = &owner
	meta["group"] = &group

	if fileInfo.IsDir() {
		t := "true"
		meta["hdi_isfolder"] = &t
	}

	totalProgress := int64(0)
	var lock sync.Mutex
	progressFunc := func(bytesTransferred int64) {
		lock.Lock()
		defer lock.Unlock()

		t := atomic.AddInt64(&totalProgress, bytesTransferred)
		util.Log(pipeline.LogDebug, fmt.Sprintf("Archiving %v: Progress %v/%v, %v %% complete",
			filepath, t, fileInfo.Size(), (float64(t)/float64(fileInfo.Size())*100.0)))
	}
	return &blockblob.UploadFileOptions{
		BlockSize: a.BlockSize,
		Metadata:  meta,
		Progress:  progressFunc,
	}, nil
}

// Archive copies local file to HNS
func Archive(ctx context.Context, copier copier.Copier, o ArchiveOptions) (int64, error) {
	logPath := o.SourcePath //hide paths till debug mode
	if util.ShouldLog(pipeline.LogDebug) {
		logPath = o.BlobName
	}

	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s", logPath))
	wg := sync.WaitGroup{}

	parents := strings.Split(o.BlobName, string(os.PathSeparator))
	parents = parents[:len(parents)-1] // Exclude the file itself (processed separately)
	wg.Add(len(parents))               // Parent directories + 1 for the file.

	filepath := o.MountRoot
	blobpath := o.ExportPrefix
	for _, currDir := range parents {
		filepath = path.Join(filepath, currDir) //keep appending path to the url
		blobpath = path.Join(blobpath, currDir)

		go func(filepath, blobpath string) {
			defer wg.Done()
			options, err := o.getUploadOptions(filepath)
			if err != nil {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving Dir %v: Failed %v", logPath, err))
				return
			}

			blob := o.ContainerURL.NewBlockBlobClient(blobpath)
			_, err = blob.UploadBuffer(ctx, nil, options)
		}(filepath, blobpath)
	}

	filepath = path.Join(o.MountRoot, o.BlobName)
	blobpath = path.Join(o.ExportPrefix, o.BlobName)
	blob := o.ContainerURL.NewBlockBlobClient(blobpath)

	options, err := o.getUploadOptions(filepath)
	if err != nil {
		return 0, err
	}

	err = copier.UploadFile(ctx, blob, filepath, options)

	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving file %v: Failed %v", logPath, err))
		return 0, err
	}

	props, err := blob.GetProperties(ctx, nil)
	if err != nil {
		util.Log(pipeline.LogError,
			fmt.Sprintf("Archiving file %v: Could not get destination length %s",
				logPath, err))
	}

	return *props.ContentLength, err
}
