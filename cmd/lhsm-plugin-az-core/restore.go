package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	AccountName     string
	BlobEndpointURL string
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
}

var maxRetryPerDownloadBody = 5

//Restore persists a blob to the local filesystem
func Restore(o RestoreOptions) (int64, error) {
	restoreCtx := context.Background()
	ctx, cancel := context.WithCancel(restoreCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{HTTPSender: util.HTTPClientFactory(o.HTTPClient)})
	blobPath := path.Join(o.ExportPrefix, o.BlobName)
	sURL, _ := url.Parse(o.BlobEndpointURL + o.ResourceSAS)
	blobURL := azblob.NewServiceURL(*sURL, p).NewContainerURL(o.ContainerName).NewBlobURL(blobPath)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring %s to %s.", blobURL.String(), o.DestinationPath))

	blobProp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, errors.Wrapf(err, "GetProperties on %s failed", o.BlobName)
	}
	contentLen := blobProp.ContentLength()
	err = util.Download(o.DestinationPath, blobPath, o.BlockSize) 

	return contentLen, err
}
