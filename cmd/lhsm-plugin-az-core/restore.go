package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"github.com/wastore/lemur/cmd/util"
)

type RestoreOptions struct {
	AccountName     string
	ContainerName   string
	BlobName        string
	DestinationPath string
	Credential      *azblob.SharedKeyCredential
	Parallelism     uint16
	BlockSize       int64
	ExportPrefix    string
	Pacer           util.Pacer
}

// persist a blob to the local filesystem
func Restore(o RestoreOptions) (int64, error) {
	restoreCtx := context.Background()
	ctx, cancel := context.WithCancel(restoreCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})

	// First, try to weed out until the symlink.
	// Symlinks are actually somewhat of an edge-case here that isn't expected,
	// But we decided that being defensive on the off chance it happens wasn't a bad idea.
	dir, fileName := filepath.Split(o.BlobName)
	paths := []string{dir + o.ExportPrefix + fileName}
	contentLen, err := int64(0), error(nil)
	util.Log(pipeline.LogInfo, fmt.Sprintf("Restoring %s", paths[len(paths)-1]))

	for {
		u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", o.AccountName, o.ContainerName, paths[len(paths)-1]))

		blobURL := azblob.NewBlobURL(*u, p)
		blobProp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
		if err != nil {
			return 0, errors.Wrapf(err, "GetProperties on %s failed", o.BlobName)
		}
		// When we exit this loop, we've either failed to get a link, or we've reached a real file.
		contentLen = blobProp.ContentLength()
		metadata := blobProp.NewMetadata()

		// TODO: Get properties, break when we hit a non-link
		if metadata["ftype"] != "LNK" {
			break
		}

		// If we got a link, read it and move forward.
		resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)

		if err != nil {
			return contentLen, err
		}

		buf, err := ioutil.ReadAll(resp.Body(azblob.RetryReaderOptions{
			MaxRetryRequests:       3,
			TreatEarlyCloseAsError: false,
		}))

		if err != nil {
			return contentLen, err
		}

		paths = append(paths, string(buf))
	}

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", o.AccountName, o.ContainerName, paths[len(paths)-1]))

	blobURL := azblob.NewBlobURL(*u, p)

	basePath := path.Dir(o.DestinationPath)

	file, _ := os.Create(path.Join(basePath, paths[len(paths)-1]))
	defer file.Close()
	err = azblob.DownloadBlobToFile(
		ctx, blobURL, 0, 0, file,
		azblob.DownloadFromBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
		})

	//paths = paths[:len(paths)-1]

	for i := len(paths) - 2; i >= 0; i-- {
		err = os.MkdirAll(path.Join(basePath, path.Dir(paths[i])), os.ModeDir|os.ModePerm)

		if err != nil {
			return contentLen, err
		}

		err = os.Symlink(path.Join(basePath, paths[i+1]), path.Join(basePath, paths[i]))

		if err != nil {
			return contentLen, err
		}
	}

	return contentLen, err
}
