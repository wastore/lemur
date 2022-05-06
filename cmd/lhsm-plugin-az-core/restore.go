package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	AccountName     string
	ContainerName   string
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
func Restore(o RestoreOptions) (int64, error) {
	restoreCtx := context.Background()
	ctx, cancel := context.WithCancel(restoreCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{HTTPSender: util.HTTPClientFactory(o.HTTPClient)})
	// We probably do not need o.ExportPrefix here. We have the full path in o.BlobName(import prefix included)
    blobPath := path.Join(o.ContainerName, o.BlobName)

	u, _ := url.Parse(fmt.Sprintf(blobEndPoint+"%s%s", o.AccountName, blobPath, o.ResourceSAS))

	util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring %s to %s.", u.String(), o.DestinationPath))

	blobURL := azblob.NewBlobURL(*u, p)
	blobProp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}
	contentLen := blobProp.ContentLength()
	err = util.Download(blobURL.String(), o.DestinationPath, o.BlockSize)

	return contentLen, err
}
