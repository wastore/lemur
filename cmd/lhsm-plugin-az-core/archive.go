package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type ArchiveOptions struct {
	AccountName   string
	ContainerName string
	ResourceSAS   string
	MountRoot     string
	BlobName      string
	SourcePath    string
	Credential    azblob.Credential
	Parallelism   uint16
	BlockSize     int64
	Pacer         util.Pacer
	ExportPrefix  string
	HNSEnabled    bool
}

var blobEndPoint string = "https://%s.blob.core.windows.net/"
var dfsEndPoint string = "https://%s.dfs.core.windows.net/"

//Archive copies local file to HNS
func Archive(o ArchiveOptions) (size int64, _ error) {
	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s", o.BlobName))
	archiveCtx := context.Background()
	ctx, cancel := context.WithCancel(archiveCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})

	cURL, _ := url.Parse(fmt.Sprintf(blobEndPoint+"%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
	containerURL := azblob.NewContainerURL(*cURL, p)

	parents := strings.Split(o.BlobName, string(os.PathSeparator))
	blobPath := o.MountRoot // Initialize the parent starting from root.
	for _, currDir := range parents {
		blobPath = path.Join(blobPath, currDir) //keep appending path to the url
		blobURL := containerURL.NewBlockBlobURL(blobPath)
		meta := azblob.Metadata{}

		//Get owner, group and perms
		fileInfo, err := os.Stat(blobPath)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get fileInfo: %s", o.BlobName, err.Error()))
			return 0, err
		}

		owner := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
		permissions := fmt.Sprintf("%o", fileInfo.Mode())
		group := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)
		modTime := fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")
		var acl string

		if o.HNSEnabled {
			dfsEP, _ := url.Parse(fmt.Sprintf(dfsEndPoint+"%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
			dfsURL := azblob.NewContainerURL(*dfsEP, p).NewBlockBlobURL(blobPath)
			aclResp, err := dfsURL.GetAccessControl(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
			if stgErr, ok := err.(azblob.StorageError); err != nil || ok && stgErr.ServiceCode() != azblob.ServiceCodeBlobNotFound {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", o.BlobName, err.Error()))
				return 0, err
			}
			if err != nil {
				acl = aclResp.XMsACL()
			}
		}

		if !o.HNSEnabled {
			meta["Permissions"] = permissions
			meta["ModTime"] = modTime
			meta["Owner"] = owner
			meta["Group"] = group
		}

		if fileInfo.IsDir() {
			meta["hdi_isfolder"] = "true"
			_, err = azblob.UploadBufferToBlockBlob(
				ctx, nil, blobURL,
				azblob.UploadToBlockBlobOptions{
					Metadata: meta,
				})
		} else {
			file, _ := os.Open(blobPath)
			defer file.Close()
			size = fileInfo.Size()
			_, err = azblob.UploadFileToBlockBlob(
				ctx, file, blobURL,
				azblob.UploadToBlockBlobOptions{
					BlockSize:   o.BlockSize,
					Parallelism: o.Parallelism,
					Metadata:    meta,
				})
		}

		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to upload blob: %s", blobPath, err.Error()))
			return 0, err
		}

		if o.HNSEnabled {
			dfsEP, _ := url.Parse(fmt.Sprintf(dfsEndPoint+"%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
			dfsURL := azblob.NewContainerURL(*dfsEP, p).NewBlockBlobURL(blobPath)
			_, err := dfsURL.SetAccessControl(ctx, nil, nil, nil, nil, nil, &acl, nil, nil, nil, nil, nil)
			if err != nil {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set Access Control: %s", blobPath, err.Error()))
				return 0, err
			}
			_, err = dfsURL.SetAccessControl(ctx, nil, nil, &owner, &group, &permissions, nil, nil, nil, nil, nil, nil)
			if err != nil {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set owner: %s", blobPath, err.Error()))
				return 0, err
			}
		}
	}

	return size, nil
}
