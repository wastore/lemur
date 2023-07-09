package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	copier "github.com/nakulkar-msft/copier/core"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	ContainerURL    container.Client
	BlobName        string
	DestinationPath string
	BlockSize       int64
	ExportPrefix    string
	Pacer           util.Pacer
	HTTPClient      *http.Client
	OpStartTime     time.Time
}

var maxRetryPerDownloadBody = 5

//Restore persists a blob to the local filesystem
func Restore(ctx context.Context, copier copier.Copier, o RestoreOptions) (int64, error) {
	blob := o.ContainerURL.NewBlockBlobClient(o.BlobName)

	if util.ShouldLog(pipeline.LogDebug) {
		util.Log(pipeline.LogDebug, fmt.Sprintf("Restoring %s to %s.", o.BlobName, o.DestinationPath))
	} else {
		util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring blob to %s.", o.DestinationPath))
	}

	size, err := copier.DownloadFile(ctx, blob, o.DestinationPath, &blob.DownloadFileOptions{})
	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Restore failed: %v", err))
		return 0, err
	}

	return size, err
}
