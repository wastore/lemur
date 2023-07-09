package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/wastore/lemur/cmd/util"
)

type RemoveOptions struct {
	ContainerURL  *container.Client
	ResourceSAS   string
	BlobName      string
	ExportPrefix  string
}

func Remove(ctx context.Context, o RemoveOptions) error {
	ctx, _ = context.WithTimeout(ctx, time.Minute * 3)
	blobPath := path.Join(o.ExportPrefix, o.BlobName)
	util.Log(pipeline.LogInfo, fmt.Sprintf("Removing %s.", blobPath))
	_, err := o.ContainerURL.NewBlobClient(blobPath).Delete(ctx, &blob.DeleteOptions{})
	return err
}
