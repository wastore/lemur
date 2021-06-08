package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type RemoveOptions struct {
	AccountName     string
	BlobEndpointURL string
	ContainerName   string
	ResourceSAS     string
	BlobName        string
	ExportPrefix    string
	Credential      azblob.Credential
}

func Remove(o RemoveOptions) error {
	ctx := context.TODO()
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})
	blobPath := path.Join(o.ExportPrefix, o.BlobName)
	sURL, _ := url.Parse(o.BlobEndpointURL + o.ResourceSAS)
	blobURL := azblob.NewServiceURL(*sURL, p).NewContainerURL(o.ContainerName).NewBlockBlobURL(blobPath)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Removing %s.", blobURL.String()))

	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
