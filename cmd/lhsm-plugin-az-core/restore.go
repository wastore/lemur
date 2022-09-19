package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	ContainerURL    *url.URL
	ResourceSAS     string
	BlobName        string
	DestinationPath string
	Credential      azblob.Credential
	Parallelism     uint16
	BlockSize       int64
	ExportPrefix    string
	Pacer           util.Pacer
	HTTPClient      *http.Client
	OpStartTime     time.Time
}

var maxRetryPerDownloadBody = 5

//Restore persists a blob to the local filesystem
func Restore(ctx context.Context, o RestoreOptions) (int64, error) {
	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{HTTPSender: util.HTTPClientFactory(o.HTTPClient)})
	containerURL := azblob.NewContainerURL(*o.ContainerURL, p)
	blobURL := containerURL.NewBlockBlobURL(o.BlobName)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring %s to %s.", blobURL.String(), o.DestinationPath))

	blobProp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}
	contentLen := blobProp.ContentLength()
	
	err = util.Download(ctx, blobURL.String(), o.DestinationPath, o.BlockSize)

	return contentLen, err
}
