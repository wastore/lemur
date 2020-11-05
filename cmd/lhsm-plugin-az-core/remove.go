package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type RemoveOptions struct {
	AccountName   string
	ContainerName string
	BlobName      string
	ExportPrefix  string
	Credential    *azblob.SharedKeyCredential
}

func Remove(o RemoveOptions) error {
	ctx := context.TODO()
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})
	dir, fileName := filepath.Split(o.BlobName)
	blobName := dir + o.ExportPrefix + fileName
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", o.AccountName, blobName))

	// fetch the properties first so that we know how big the source blob is
	blobURL := azblob.NewBlobURL(*u, p)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
