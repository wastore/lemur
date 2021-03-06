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

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type ArchiveOptions struct {
	AccountName     string
	BlobEndpointURL string
	DFSEndpointURL  string
	ContainerName   string
	ResourceSAS     string
	MountRoot       string
	BlobName        string
	SourcePath      string
	Credential      azblob.Credential
	Parallelism     uint16
	BlockSize       int64
	Pacer           util.Pacer
	ExportPrefix    string
	HNSEnabled      bool
	HTTPClient      *http.Client
}

const parallelDirCount = 64 // Number parallel dir metadata uploads

func upload(ctx context.Context, o ArchiveOptions, blobPath string) (_ int64, err error) {
	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{HTTPSender: util.HTTPClientFactory(o.HTTPClient)})
	var aclResp *azblob.BlobGetAccessControlResponse
	sURL, _ := url.Parse(o.BlobEndpointURL + o.ResourceSAS)
	dfsServiceURL, _ := url.Parse(o.DFSEndpointURL + o.ResourceSAS)
	blobURL := azblob.NewServiceURL(*sURL, p).NewContainerURL(o.ContainerName).NewBlockBlobURL(path.Join(o.ExportPrefix, blobPath))
	dfsURL := azblob.NewServiceURL(*dfsServiceURL, p).NewContainerURL(o.ContainerName).NewBlockBlobURL(path.Join(o.ExportPrefix, blobPath))
	meta := azblob.Metadata{}

	getACLResp := func(path string) (*azblob.BlobGetAccessControlResponse, error) {
		return dfsURL.GetAccessControl(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
	}

	setACLResp := func(path string) error {
		acl := aclResp.XMsACL()
		owner := aclResp.XMsOwner()
		group := aclResp.XMsGroup()
		permissions := aclResp.XMsPermissions()
		_, err := dfsURL.SetAccessControl(ctx, nil, nil, nil, nil, nil, &acl, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set Access Control: %s", blobPath, err.Error()))
			return err
		}
		_, err = dfsURL.SetAccessControl(ctx, nil, nil, &owner, &group, &permissions, nil, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set owner: %s", blobPath, err.Error()))
			return err
		}
		return nil
	}

	//Get owner, group and perms
	fileInfo, err := os.Stat(path.Join(o.MountRoot, blobPath))
	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get fileInfo: %s", blobPath, err.Error()))
		return 0, err
	}

	owner := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	permissions := uint32(fileInfo.Mode().Perm())
	if fileInfo.Mode()&os.ModeSticky != 0 {
		permissions |= syscall.S_ISVTX
	}
	group := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)
	modTime := fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")

	if o.HNSEnabled {
		aclResp, err = getACLResp(blobPath)
		if stgErr, ok := err.(azblob.StorageError); err != nil || ok && stgErr.ServiceCode() != azblob.ServiceCodeBlobNotFound {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", dfsURL.URL().Path, err.Error()))
			return 0, err
		}
	}

	meta["permissions"] = fmt.Sprintf("%04o", permissions)
	meta["modtime"] = modTime
	meta["owner"] = owner
	meta["group"] = group

	if fileInfo.IsDir() {
		meta["hdi_isfolder"] = "true"
		_, err = blobURL.Upload(ctx, bytes.NewReader(nil), azblob.BlobHTTPHeaders{}, meta, azblob.BlobAccessConditions{}, azblob.AccessTierNone)
	} else {
		fi, _ := os.Stat(path.Join(o.MountRoot, blobPath))
		file, _ := os.Open(path.Join(o.MountRoot, blobPath))
		defer file.Close()

		_, err = azblob.UploadFileToBlockBlob(
			ctx, file, blobURL,
			azblob.UploadToBlockBlobOptions{
				BlockSize:   util.GetBlockSize(fi.Size(), o.BlockSize),
				Parallelism: o.Parallelism,
				Metadata:    meta,
			})
	}

	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to upload blob: %s", blobPath, err.Error()))
		return 0, err
	}

	if o.HNSEnabled && aclResp != nil {
		err = setACLResp(blobPath)
		if err != nil {
			return 0, err
		}
	}

	return fileInfo.Size(), err
}

//Archive copies local file to HNS
func Archive(o ArchiveOptions) (size int64, err error) {
	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s", o.BlobName))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
