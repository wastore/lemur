package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	AccountName     string
	ContainerName   string
	BlobName        string
	DestinationPath string
	Credential      *azblob.SharedKeyCredential
	Parallelism     uint16
	BlockSize       int64
	ExportPrefix    string
	Pacer           util.Pacer
}

// persist a blob to the local filesystem
func Restore(o RestoreOptions) (int64, error) {
	restoreCtx := context.Background()
	ctx, cancel := context.WithCancel(restoreCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})

	dir, fileName := filepath.Split(o.BlobName)
	blobName := dir + o.ExportPrefix + fileName

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s%s", o.AccountName, o.ContainerName, blobName))

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
		})

	return contentLen, err
}
