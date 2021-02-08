package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
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
}

var maxRetryPerDownloadBody = 5

//Restore persists a blob to the local filesystem
func Restore(o RestoreOptions) (int64, error) {
	restoreCtx := context.Background()
	ctx, cancel := context.WithCancel(restoreCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})
	blobPath := path.Join(o.ContainerName, o.ExportPrefix, o.BlobName)

	u, _ := url.Parse(fmt.Sprintf(blobEndPoint+"%s%s", o.AccountName, blobPath, o.ResourceSAS))

	util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring %s to %s.", u.String(), o.DestinationPath))

	blobURL := azblob.NewBlobURL(*u, p)
	blobProp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return 0, errors.Wrapf(err, "GetProperties on %s failed", o.BlobName)
	}
	contentLen := blobProp.ContentLength()

	file, _ := os.Create(o.DestinationPath)
	defer file.Close()
	err = azblob.DownloadBlobToFile(
		ctx, blobURL, 0, 0, file,
		azblob.DownloadFromBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
			RetryReaderOptionsPerBlock: azblob.RetryReaderOptions{
				MaxRetryRequests: maxRetryPerDownloadBody,
				NotifyFailedRead: util.NewReadLogFunc(u.String()),
			},
		})

	return contentLen, err
}
