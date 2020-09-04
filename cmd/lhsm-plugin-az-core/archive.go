package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type ArchiveOptions struct {
	AccountName   string
	ContainerName string
	BlobName      string
	SourcePath    string
	Credential    *azblob.SharedKeyCredential
	Parallelism   uint16
	BlockSize     int64
}

// persist a blob to the local filesystem
func Archive(o ArchiveOptions) (int64, error){
	ctx := context.TODO()
	p := azblob.NewPipeline(o.Credential, azblob.PipelineOptions{})

	// We might actually encounter symlinks here.
	paths := []string{o.SourcePath}

	for {
		idx := len(paths)-1

		// In the original code, the error was ignored, so we're going to maintain the assumption that it's OK to ignore the error here
		// Though it's not really the most agreeable thing.
		fi, _ := os.Lstat(paths[idx])

		// If the file is not a symlink, we can jump to uploading it.
		if fi.Mode() & os.ModeSymlink == 0 {
			break
		}

		// Otherwise, read the link and append it.
		next, err := os.Readlink(paths[idx])

		if err != nil {
			return 0, err
		}

		paths = append(paths, next)
	}

	// Currently, we know the absolute paths, with 0 being the first link in the chain and n being the file itself.
	// Let's decipher what the actual root is, so we know where our blobs live.
	// Each of our blob names are going to be strings.TrimPrefix(path, fsRootPath)
	fsRootPath := strings.TrimSuffix(filepath.Dir(o.SourcePath), filepath.Dir(o.BlobName)) + "/"

	// First, upload the file... We know exactly where this is and what it's going to be named, thus, we'll fix paths[n] so that links go right.

	//// First, upload the file. TODO: Detect if the file and links already exist
	//// To do this, we need to know the relative paths...

	cURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", o.AccountName, o.ContainerName))
	containerURL := azblob.NewContainerURL(*cURL, p)
	blobURL := containerURL.NewBlockBlobURL(strings.TrimSuffix(strings.TrimPrefix(paths[len(paths)-1], fsRootPath), "/"))

	// open the file to read from
	file, _ := os.Open(paths[len(paths)-1])
	fileInfo, _ := file.Stat()
	defer file.Close()

	total := fileInfo.Size()
	meta := azblob.Metadata{}

	meta["Permissions"] = fmt.Sprintf("%o", fileInfo.Mode())
	meta["ModTime"] = fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")
	meta["Owner"] = fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	meta["Group"] = fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)

	_, err := azblob.UploadFileToBlockBlob(
		ctx,
		file,
		blobURL,
		azblob.UploadToBlockBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
			Metadata:    meta,
		})

	if err != nil {
		return 0, err
	}

	//// Prior to creating the links, we need to change what the first link is to match the new destination.
	//// @Narasimha, this should help you with supporting the custom names.
	//paths[len(paths)-1] = path.Join(fsRootPath, o.BlobName)

	// Then, create the links. We do it backwards, much like we do for a download.
	for idx := len(paths) - 2; idx >= 0; idx-- {
		linkName := strings.TrimPrefix(paths[idx], fsRootPath)
		linkDest := strings.TrimPrefix(paths[idx+1], fsRootPath)

		_, err = azblob.UploadBufferToBlockBlob(
			ctx,
			[]byte(linkDest),
			containerURL.NewBlockBlobURL(linkName),
			azblob.UploadToBlockBlobOptions{
				Metadata: azblob.Metadata{ "ftype":"LNK" },
			},
		)

		if err != nil {
			return 0, err
		}
	}

	return total, err
}