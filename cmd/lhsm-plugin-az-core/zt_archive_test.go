package lhsm_plugin_az_core

import (
	"os"
	"path/filepath"

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
