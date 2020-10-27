package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

	// We might actually encounter symlinks here.
	paths := []string{o.BlobName}

	for {
		idx := len(paths) - 1

		// In the original code, the error was ignored, so we're going to maintain the assumption that it's OK to ignore the error here
		// Though it's not really the most agreeable thing.
		fi, _ := os.Lstat(paths[idx])

		// If the file is not a symlink, we can jump to uploading it.
		if fi.Mode()&os.ModeSymlink == 0 {
			break
		}

		// Otherwise, read the link and append it.
		next, err := os.Readlink(paths[idx])

		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Failed to read link. %s", err.Error()))
			return 0, err
		}

		paths = append(paths, next)
	}

	// Currently, we know the absolute paths, with 0 being the first link in the chain and n being the file itself.
	// Let's decipher what the actual root is, so we know where our blobs live.
	// Each of our blob names are going to be strings.TrimPrefix(path, fsRootPath)
	fsRootPath := filepath.Dir(o.BlobName) + "/"

	// First, upload the file... We know exactly where this is and what it's going to be named, thus, we'll fix paths[n] so that links go right.

	//// First, upload the file. TODO: Detect if the file and links already exist
	//// To do this, we need to know the relative paths...

	cURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", o.AccountName, o.ContainerName))
	containerURL := azblob.NewContainerURL(*cURL, p)
	dir, fileName := filepath.Split(o.BlobName)
	blobURL := containerURL.NewBlockBlobURL(dir + o.ExportPrefix + fileName)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s to %s.", o.BlobName, blobURL.String()))

	// open the file to read from
	file, _ := os.Open(paths[len(paths)-1])
	fileInfo, _ := file.Stat()
	defer file.Close()

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
		aclResp, err := blobURL.GetAccessControl(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
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
		_, err = blobURL.SetAccessControl(ctx, nil, nil, &owner, &group, &permissions, &acl, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set AccessControl: %s", o.BlobName, err.Error()))
			//TODO: should we delete blob?
		}
	}

	//// Prior to creating the links, we need to change what the first link is to match the new destination.
	//// @Narasimha, this should help you with supporting the custom names.
	//paths[len(paths)-1] = path.Join(fsRootPath, o.BlobName)

	// Then, create the links. We do it backwards, much like we do for a download.
	for idx := len(paths) - 2; idx >= 0; idx-- {
		linkName := strings.TrimPrefix(paths[idx], fsRootPath)
		linkDest := strings.TrimPrefix(paths[idx+1], fsRootPath)
		dir, link := filepath.Split(linkName)

		_, err = azblob.UploadBufferToBlockBlob(
			ctx,
			[]byte(linkDest),
			containerURL.NewBlockBlobURL(dir+o.ExportPrefix+link),
			azblob.UploadToBlockBlobOptions{
				Metadata: azblob.Metadata{"ftype": "LNK"},
			},
		)

		if err != nil {
			return 0, err
		}
	}

	return total, err
}
