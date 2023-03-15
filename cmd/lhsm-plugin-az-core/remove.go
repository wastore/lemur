package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type RemoveOptions struct {
	ContainerURL  *url.URL
	ResourceSAS   string
	BlobName      string
	ExportPrefix  string
	Credential    azblob.Credential
	OpStartTime   time.Time
}

func Remove(ctx context.Context, o RemoveOptions) error {
	ctx, _ = context.WithTimeout(ctx, time.Minute * 3)
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})
	blobPath := path.Join(o.ExportPrefix, o.BlobName)
	blobURL := azblob.NewContainerURL(*o.ContainerURL, p).NewBlockBlobURL(blobPath)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Removing %s.", blobURL.String()))

	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
