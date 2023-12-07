package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	copier "github.com/wastore/lemur/copier/core"
	"github.com/wastore/lemur/cmd/util"
	chk "gopkg.in/check.v1"
)

// test upload/download with the source data uploaded to the service from a file
// this is a round-trip test where both upload and download are exercised together
func performUploadAndDownloadFileTest(c *chk.C, fileSize, blockSize, parallelism int) {
	copier := copier.NewCopier(0, maxBlockLength, defaultCachelimit, defaultConcurrency)
	// Set up file to upload
	fileName := generateName("", 0)
	filePath := filepath.Join(os.TempDir(), fileName)
	fileData := generateFile(filePath, fileSize)

	// Open the file to upload
	file, err := os.Open(filePath)
	c.Assert(err, chk.IsNil)
	defer file.Close()
	defer os.Remove(filePath)

	// Set up test container
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer bsu.DeleteContainer(context.TODO(), containerName, nil)
	//	_, err  = containerURL.Delete(context.TODO(), nil)

	//setup logger
	util.InitJobLogger(pipeline.LogDebug)

	// Upload the file to a block blob
	c.Assert(err, chk.IsNil)
	count, err := Archive(context.Background(), copier, ArchiveOptions{
		ContainerURL: containerURL,
		BlobName:     fileName,
		SourcePath:   fileName,
		BlockSize:    int64(blockSize),
		MountRoot:    os.TempDir(),
		HTTPClient:   &http.Client{},
	})
	c.Assert(err, chk.Equals, nil)
	c.Assert(count, chk.Equals, int64(fileSize))

	// Set up file to download the blob to
	destFileName := fileName + "-downloaded"
	destFilePath := filepath.Join(os.TempDir(), destFileName)
	destFile, err := os.Create(destFilePath)
	c.Assert(err, chk.Equals, nil)
	defer destFile.Close()
	defer os.Remove(destFilePath)

	blobName := fileName
	// invoke restore to download the file back
	count, err = Restore(context.Background(), copier, RestoreOptions{
		ContainerURL:    containerURL,
		BlobName:        blobName,
		DestinationPath: destFilePath,
		BlockSize:       int64(blockSize),
		HTTPClient:      &http.Client{},
	})

	// Assert download was successful
	c.Assert(err, chk.Equals, nil)
	c.Assert(count, chk.Equals, int64(fileSize))

	// Assert downloaded data is consistent
	destBuffer := make([]byte, count)
	n, err := destFile.Read(destBuffer)
	c.Assert(err, chk.Equals, nil)
	c.Assert(n, chk.Equals, fileSize)
	c.Assert(destBuffer, chk.DeepEquals, fileData)
}

func (s *cmdIntegrationSuite) TestUploadAndDownloadFileSingleIO(c *chk.C) {
	fileSize := 1024
	blockSize := 2048
	parallelism := 3
	performUploadAndDownloadFileTest(c, fileSize, blockSize, parallelism)
}

func (_ *cmdIntegrationSuite) TestPreservePermsRecursive(c *chk.C) {
	copier := copier.NewCopier(0, maxBlockLength, defaultCachelimit, defaultConcurrency)
	fileName := generateName("", 0)
	fileSize := 1024
	tempDir := os.TempDir()
	pathWithManyDirs := "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/"
	filePath := filepath.Join(pathWithManyDirs, fileName)

	err := os.MkdirAll(filepath.Join(tempDir, pathWithManyDirs), 0777)
	c.Assert(err, chk.IsNil)

	fileName = filepath.Join(pathWithManyDirs, fileName)
	filePath = filepath.Join(tempDir, fileName)
	generateFile(filePath, fileSize)

	// Open the file to upload
	file, err := os.Open(filePath)
	c.Assert(err, chk.IsNil)
	defer file.Close()
	defer os.Remove(filePath)

	// Set up test container
	bsu := getBSU()
	containerURL, cName := createNewContainer(c, bsu)
	defer bsu.DeleteContainer(context.TODO(), cName, nil)

	//setup logging
	util.InitJobLogger(pipeline.LogDebug)

	c.Assert(err, chk.IsNil)
	count, err := Archive(context.Background(), copier, ArchiveOptions{
		ContainerURL: containerURL,
		BlobName:     fileName,
		SourcePath:   fileName,
		BlockSize:    int64(2048), //2M
		MountRoot:    tempDir,
		HTTPClient:   &http.Client{},
	})
	c.Assert(err, chk.Equals, nil)
	c.Assert(count, chk.Equals, int64(fileSize))

	blobs := strings.Split(fileName, string(os.PathSeparator))
	blobname := ""

	for _, curr := range blobs {
		blobname = filepath.Join(blobname, curr)
		blobURL := containerURL.NewBlockBlobClient(blobname)
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Minute)
		info, err := os.Stat(filepath.Join(tempDir, blobname))
		owner := fmt.Sprintf("%d", info.Sys().(*syscall.Stat_t).Uid)
		permissions := uint32(info.Mode().Perm())
		if info.Mode()&os.ModeSticky != 0 {
			permissions |= syscall.S_ISVTX
		}
		group := fmt.Sprintf("%d", info.Sys().(*syscall.Stat_t).Gid)

		props, err := blobURL.GetProperties(ctx, nil)
		c.Assert(err, chk.Equals, nil)

		meta := props.Metadata
		c.Assert(*(meta["Owner"]), chk.Equals, owner)
		c.Assert(*(meta["Group"]), chk.Equals, group)

		blobPerms, _ := strconv.ParseUint(*(meta["Permissions"]), 8, 32)
		c.Assert(blobPerms, chk.Equals, uint64(permissions))
	}
}
