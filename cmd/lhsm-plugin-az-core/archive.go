package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"syscall"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type ArchiveOptions struct {
	AccountName   string
	ContainerName string
	BlobName      string
	SourcePath    string
	Credential    *azblob.SharedKeyCredential
	Parallelism   uint16
	BlockSize     int64
	Pacer         util.Pacer
}

// persist a blob to the local filesystem
func Archive(o ArchiveOptions) (int64, error) {
	archiveCtx := context.Background()
	ctx, cancel := context.WithCancel(archiveCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})
	cURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", o.AccountName, o.ContainerName))
	containerURL := azblob.NewContainerURL(*cURL, p)
	blobURL := containerURL.NewBlockBlobURL(o.BlobName)

	// open the file to read from
	file, _ := os.Open(o.SourcePath)
	fileInfo, _ := file.Stat()
	defer file.Close()

	total := fileInfo.Size()
	meta := azblob.Metadata{}

	meta["Permissions"] = fmt.Sprintf("%o", fileInfo.Mode())
	meta["ModTime"] = fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")
	meta["Owner"] = fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	meta["Group"] = fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)

	_, err := azblob.UploadFileToBlockBlob(
		ctx,
		file,
		blobURL,
		azblob.UploadToBlockBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
			Metadata:    meta,
		})

	return total, err
}
