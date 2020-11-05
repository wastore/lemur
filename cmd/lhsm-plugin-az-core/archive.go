package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/Azure/azure-pipeline-go/pipeline"
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
	ExportPrefix  string
}

// persist a blob to the local filesystem
func Archive(o ArchiveOptions) (int64, error) {
	archiveCtx := context.Background()
	ctx, cancel := context.WithCancel(archiveCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})

	//Get the blob URL
	cURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", o.AccountName, o.ContainerName))
	containerURL := azblob.NewContainerURL(*cURL, p)
	dir, fileName := filepath.Split(o.BlobName)
	blobName := dir + o.ExportPrefix + fileName
	blobURL := containerURL.NewBlockBlobURL(blobName)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s to %s.", o.BlobName, blobURL.String()))

	// open the file to read from
	file, _ := os.Open(o.SourcePath)
	fileInfo, _ := file.Stat()
	defer file.Close()

	//Save owner, perm, group and acl info
	total := fileInfo.Size()
	meta := azblob.Metadata{}
	owner := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	permissions := fmt.Sprintf("%o", fileInfo.Mode())
	group := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)
	var acl string
	modTime := fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")

	resp, err := blobURL.GetAccountInfo(context.Background())
	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to account info: %s", o.BlobName, err.Error()))
		return 0, err
	}

	hnsEnabledAccount := resp.Response().Header.Get("X-Ms-Is-Hns-Enabled") == "true"
	if hnsEnabledAccount {
		u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s", o.AccountName, o.ContainerName, blobName))
		dfsURL := azblob.NewBlockBlobURL(*u, p)
		aclResp, err := dfsURL.GetAccessControl(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", o.BlobName, err.Error()))
			return 0, err
		}
		acl = aclResp.XMsACL()
	}

	if !hnsEnabledAccount {
		meta["Permissions"] = permissions
		meta["ModTime"] = modTime
		meta["Owner"] = owner
		meta["Group"] = group
	}

	_, err = azblob.UploadFileToBlockBlob(
		ctx,
		file,
		blobURL,
		azblob.UploadToBlockBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
			Metadata:    meta,
		})

	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to upload blob: %s", o.BlobName, err.Error()))
		return 0, err
	}

	if hnsEnabledAccount {
		u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s", o.AccountName, o.ContainerName, blobName))
		dfsURL := azblob.NewBlockBlobURL(*u, p)
		/*
			_, err = dfsURL.SetAccessControl(ctx, nil, nil, &owner, &group, &permissions, nil, nil, nil, nil, nil, nil)
			if err != nil {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s, Failed to set owner, group and permissions: %s", o.BlobName, err.Error()))
				return 0, err
			}
		*/
		_, err = dfsURL.SetAccessControl(ctx, nil, nil, nil, nil, nil, &acl, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set AccessControl: %s", o.BlobName, err.Error()))
			//TODO: should we delete blob?
		}
	}

	return total, err
}
