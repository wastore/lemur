package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"net/url"
	"os"
)

type RestoreOptions struct {
	AccountName string
	ContainerName string
	BlobName string
	DestinationPath string
	Credential *azblob.SharedKeyCredential
	Parallelism uint16
	BlockSize int64
}

// persist a blob to the local filesystem
func Restore(o RestoreOptions) (int64, error){
	ctx := context.TODO()
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", o.AccountName, o.ContainerName, o.BlobName))

	// fetch the properties first so that we know how big the source blob is
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
			BlockSize:  o.BlockSize,
			Parallelism: o.Parallelism,
		})

	return contentLen, err
}
