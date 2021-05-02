package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/wastore/lemur/cmd/util"
)

type RemoveOptions struct {
	AccountName   string
	ContainerName string
	ResourceSAS   string
	BlobName      string
	ExportPrefix  string
	Credential    azblob.Credential
	Environment   *azure.Environment
}

func Remove(o RemoveOptions) error {
	ctx := context.TODO()
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})
	blobPath := path.Join(o.ContainerName, o.ExportPrefix, o.BlobName)
	u, _:= url.Parse(fmt.Sprintf("https://%s.blob.%s/%s%s", o.AccountName,
								o.Environment.StorageEndpointSuffix, blobPath, o.ResourceSAS))

	util.Log(pipeline.LogInfo, fmt.Sprintf("Removing %s.", u.String()))

	// fetch the properties first so that we know how big the source blob is
	blobURL := azblob.NewBlobURL(*u, p)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
