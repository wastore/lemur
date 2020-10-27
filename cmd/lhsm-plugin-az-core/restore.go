package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
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
	blobName := []string{dir + o.ExportPrefix + fileName}
	contentLen, err := int64(0), error(nil)

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", o.AccountName, o.ContainerName, blobName))

	blobURL := azblob.NewBlobURL(*u, p)
	util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring %s to %s", blobURL.String(), o.BlobName))

	basePath := path.Dir(o.DestinationPath)

	file, _ := os.Create(path.Join(basePath, o.BlobName))
	defer file.Close()
	err = azblob.DownloadBlobToFile(
		ctx, blobURL, 0, 0, file,
		azblob.DownloadFromBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
		})

	return contentLen, err
}
