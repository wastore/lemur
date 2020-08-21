package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
)

type RemoveOptions struct {
	AccountName string
	ContainerName string
	BlobName string
	Credential *azblob.SharedKeyCredential
}

func Remove(o RemoveOptions) error {
	ctx := context.TODO()
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", o.AccountName, o.ContainerName, o.BlobName))

	// fetch the properties first so that we know how big the source blob is
	blobURL := azblob.NewBlobURL(*u, p)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
