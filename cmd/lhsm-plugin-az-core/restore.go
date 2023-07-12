package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	copier "github.com/nakulkar-msft/copier/core"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	ContainerURL    *container.Client
	BlobName        string
	DestinationPath string
	BlockSize       int64
	ExportPrefix    string
	HTTPClient      *http.Client
	OpStartTime     time.Time
}

var maxRetryPerDownloadBody = 5

//Restore persists a blob to the local filesystem
func Restore(ctx context.Context, copier copier.Copier, o RestoreOptions) (int64, error) {
	b := o.ContainerURL.NewBlockBlobClient(o.BlobName)

	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("Restoring %s to %s.", o.BlobName, o.DestinationPath))
	} else {
		util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring blob to %s.", o.DestinationPath))
	}

	stat, err := os.Stat(o.DestinationPath)
	if err != nil {
		return 0, err
	}

	totalProgres := int64(0)
	var lock sync.Mutex
	progressFunc := func(bytesTransferred int64) {
		lock.Lock()
		defer lock.Unlock()

		t := atomic.AddInt64(&totalProgres, bytesTransferred)
		util.Log(pipeline.LogDebug, fmt.Sprintf("Restoring %v, Progress %v/%v, %v %% complete",
				 o.DestinationPath, t, stat.Size(), (float64(t)/float64(stat.Size()) * 100.0)))
	}


	options := blob.DownloadFileOptions{
		BlockSize: o.BlockSize,
		Progress: progressFunc,
	}
	size, err := copier.DownloadFile(ctx, b, o.DestinationPath, &options)
	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Restore failed: %v", err))
		return 0, err
	}

	return size, err
}
