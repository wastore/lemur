package lhsm_plugin_az_core

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
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
	HTTPClient    *http.Client
	OpStartTime   time.Time
}

const blobEndPoint string = "https://%s.blob.core.windows.net/"
const dfsEndPoint string = "https://%s.dfs.core.windows.net/"
const parallelDirCount = 64 // Number parallel dir metadata uploads

func upload(ctx context.Context, o ArchiveOptions, blobPath string) (_ int64, err error) {
	filepath := path.Join(o.MountRoot, blobPath)
	blobPath = path.Join(o.ExportPrefix, blobPath)

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{HTTPSender: util.HTTPClientFactory(o.HTTPClient)})
	cURL, _ := url.Parse(fmt.Sprintf(blobEndPoint+"%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
	containerURL := azblob.NewContainerURL(*cURL, p)
	blobURL := containerURL.NewBlockBlobURL(blobPath)
	meta := azblob.Metadata{}

	//Get owner, group and perms
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get fileInfo: %s", filepath, err.Error()))
		return 0, err
	}

	owner := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	permissions := uint32(fileInfo.Mode().Perm())
	if fileInfo.Mode()&os.ModeSticky != 0 {
		permissions |= syscall.S_ISVTX
	}
	group := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)
	modTime := fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")
	var getACLResp azbfs.BlobFSAccessControl

	if o.HNSEnabled {
		dfsEP, _ := url.Parse(fmt.Sprintf(dfsEndPoint+"%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
		fsURL := azbfs.NewFileSystemURL(*dfsEP, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))
		fileURL := fsURL.NewRootDirectoryURL().NewFileURL(blobPath)
		
		getACLResp, err = fileURL.GetAccessControl(ctx)
		if stgErr, ok := err.(azbfs.StorageError); err != nil && ok && stgErr.Response().StatusCode != http.StatusNotFound {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s BlobPath %s", fileURL.URL().Path, err.Error(), blobPath))
			return 0, err
		}
	} else {
		meta["permissions"] = fmt.Sprintf("%04o", permissions)
		meta["modtime"] = modTime
		meta["owner"] = owner
		meta["group"] = group
	}

	if fileInfo.IsDir() {
		meta["hdi_isfolder"] = "true"
		_, err = blobURL.Upload(ctx, bytes.NewReader(nil), azblob.BlobHTTPHeaders{}, meta, azblob.BlobAccessConditions{}, azblob.AccessTierNone, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	} else {	
		err = util.Upload(ctx, filepath, blobURL.String(), o.BlockSize, meta)
	}

	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to upload blob: %s", blobPath, err.Error()))
		return 0, err
	}

	if o.HNSEnabled && (getACLResp.ACL != "" || getACLResp.Group != "" || getACLResp.Owner != "") {
		dfsEP, _ := url.Parse(fmt.Sprintf(dfsEndPoint+"%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
		fsURL := azbfs.NewFileSystemURL(*dfsEP, p)
		fileURL := fsURL.NewRootDirectoryURL().NewFileURL(blobPath)

		getACLResp.Permissions = ""
		_, err := fileURL.SetAccessControl(ctx, getACLResp)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set AccessControl: %s", blobPath, err.Error()))
			return 0, err
		}
	}

	return fileInfo.Size(), err
}

//Archive copies local file to HNS
func Archive(ctx context.Context, o ArchiveOptions) (size int64, err error) {
	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s", o.BlobName))
	wg := sync.WaitGroup{}

	parents := strings.Split(o.BlobName, string(os.PathSeparator))
	parents = parents[:len(parents)-1] // Exclude the file itself (processed separately)
	wg.Add(len(parents) + 1)           // Parent directories + 1 for the file.

	// Upload the file
	go func() {
		size, err = upload(ctx, o, o.BlobName)
		wg.Done()
	}()

	//parallely upload directories starting from root.
	guard := make(chan struct{}, parallelDirCount) //Guard maintains bounded paralleism for uploading directories
	defer close(guard)

	blobPath := ""
	for _, currDir := range parents {
		blobPath = path.Join(blobPath, currDir) //keep appending path to the url

		guard <- struct{}{} //block till we've enough room.
		go func(p string) {
			upload(ctx, o, p)
			<-guard //release a space in guard.
			wg.Done()
		}(blobPath)
	}

	wg.Wait()

	return size, err
}
