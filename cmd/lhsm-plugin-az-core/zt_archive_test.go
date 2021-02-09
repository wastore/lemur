package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

// test upload/download with the source data uploaded to the service from a file
// this is a round-trip test where both upload and download are exercised together
func performUploadAndDownloadFileTest(c *chk.C, fileSize, blockSize, parallelism int) {
	// Set up file to upload
	fileName := generateName("", 0)
	filePath := filepath.Join(os.TempDir(), fileName)
	fileData := generateFile(filePath, fileSize)

	// Open the file to upload
	file, err := os.Open(filePath)
	c.Assert(err, chk.IsNil)
	defer file.Close()
	defer os.Remove(fileName)

	// Set up test container
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Upload the file to a block blob
	account, key := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(account, key)
	c.Assert(err, chk.IsNil)
	count, err := Archive(ArchiveOptions{
		AccountName:   account,
		ContainerName: containerName,
		BlobName:      fileName,
		SourcePath:    filePath,
		Credential:    credential,
		Parallelism:   uint16(parallelism),
		BlockSize:     int64(blockSize),
		MountRoot:     os.TempDir(),
	})
	c.Assert(err, chk.Equals, nil)
	c.Assert(count, chk.Equals, int64(fileSize))

	// Set up file to download the blob to
	destFileName := fileName + "-downloaded"
	destFilePath := filepath.Join(os.TempDir(), destFileName)
	destFile, err := os.Create(destFilePath)
	c.Assert(err, chk.Equals, nil)
	defer destFile.Close()
	defer os.Remove(destFileName)

	blobName := containerName + "/" + fileName
	// invoke restore to download the file back
	count, err = Restore(RestoreOptions{
		AccountName:     account,
		ContainerName:   "",
		BlobName:        blobName,
		DestinationPath: destFilePath,
		Credential:      credential,
		Parallelism:     uint16(parallelism),
		BlockSize:       int64(blockSize),
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
	fileName := generateName("", 0)
	fileSize := 1024
	tempDir := "/home/nakulkar/tmp/"
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
	defer os.Remove(fileName)

	// Set up test container
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Upload the file to a block blob
	account, key := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(account, key)
	c.Assert(err, chk.IsNil)
	count, err := Archive(ArchiveOptions{
		AccountName:   account,
		ContainerName: containerName,
		BlobName:      fileName,
		SourcePath:    filePath,
		Credential:    credential,
		Parallelism:   uint16(3),
		BlockSize:     int64(2048), //2M
		MountRoot:     tempDir,
	})
	c.Assert(err, chk.Equals, nil)
	c.Assert(count, chk.Equals, int64(fileSize))

	blobs := strings.Split(fileName, string(os.PathSeparator))
	blobname := ""

	for _, curr := range blobs {
		blobname = filepath.Join(blobname, curr)
		blobURL := containerURL.NewBlockBlobURL(blobname)
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Minute)
		info, err := os.Stat(filepath.Join(tempDir, blobname))
		owner := fmt.Sprintf("%d", info.Sys().(*syscall.Stat_t).Uid)
		permissions := info.Mode()
		group := fmt.Sprintf("%d", info.Sys().(*syscall.Stat_t).Gid)

		props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
		c.Assert(err, chk.Equals, nil)

		meta := props.NewMetadata()
		c.Assert(meta["owner"], chk.Equals, owner)
		c.Assert(meta["group"], chk.Equals, group)

		blobPerms, _ := strconv.ParseUint(meta["permissions"], 8, 32)
		c.Assert(os.FileMode(blobPerms), chk.Equals, permissions)
	}
}
