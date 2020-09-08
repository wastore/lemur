package lhsm_plugin_az_core

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func performUploadAndDownloadSymlinkTest(c *chk.C, symlinkCount, fileSize, blockSize, parallelism int) {
	fileName := generateName("", 0)
	dir, err := ioutil.TempDir("", "upload*")
	c.Assert(err, chk.IsNil)

	fileData := generateFile(path.Join(dir, fileName), fileSize)

	// make the tmp directory and the subdirectory.
	os.MkdirAll(filepath.Join(dir, "subdir"), os.ModeDir | os.ModePerm)
	defer os.RemoveAll(dir)
	filePath := filepath.Join(dir, fileName)

	lastLink := filePath
	linkName := ""
	for i := symlinkCount; i > 0; i-- {
		linkName = generateName("link", 0)
		var newLink string

		if i % 2 == 0 {
			newLink = filepath.Join(dir, "subdir", linkName)
		} else {
			newLink = filepath.Join(dir, linkName)
		}

		c.Assert(os.Symlink(lastLink, newLink), chk.IsNil)

		lastLink = newLink
	}
	c.Assert(lastLink, chk.Not(chk.Equals), filePath)
	c.Assert(linkName, chk.Not(chk.Equals), "")

	// Set up the test container
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Upload the file, targeting the symlink (intended functionality)
	account, key := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(account, key)
	c.Assert(err, chk.IsNil)
	count, err := Archive(ArchiveOptions{
		AccountName: account,
		ContainerName: containerName,
		BlobName: linkName,
		SourcePath: lastLink,
		Credential: credential,
		Parallelism: uint16(parallelism),
		BlockSize: int64(blockSize),
	})
	c.Assert(err, chk.IsNil)
	c.Assert(count, chk.Equals, int64(len(fileData)))

	// Set up the downloads.
	dlDir, err := ioutil.TempDir("", "download*")
	c.Assert(err, chk.IsNil)
	os.MkdirAll(dlDir, os.ModeDir)
	defer os.RemoveAll(dlDir)
	destFilePath := filepath.Join(dlDir, linkName)

	// invoke restore
	count, err = Restore(RestoreOptions{
		AccountName:     account,
		ContainerName:   containerName,
		BlobName:        strings.TrimPrefix(lastLink, dir + "/"),
		DestinationPath: destFilePath,
		Credential:      credential,
		Parallelism:     uint16(parallelism),
		BlockSize:       int64(blockSize),
	})

	// Assert download was successful
	c.Assert(err, chk.Equals, nil)
	c.Assert(count, chk.Equals, int64(len(fileData)))

	// Assert the folder was the same, and that the file is the same.
	parameterizeWalkFunc := func(fileMap map[string]bool, rootPath string) filepath.WalkFunc {
		return func(path string, info os.FileInfo, err error) error {
			// skip the root
			if path == rootPath {
				return err
			}

			fileMap[strings.TrimPrefix(path, rootPath)] = true

			return err
		}
	}

	dlMap := make(map[string]bool)
	filepath.Walk(dlDir, parameterizeWalkFunc(dlMap, dlDir))
	ulMap := make(map[string]bool)
	filepath.Walk(dir, parameterizeWalkFunc(ulMap, dir))

	for k,_ := range dlMap {
		if _, ok := ulMap[k]; !ok {
			// If you fail here, one map lacks something.
			c.Fail()
		}
	}

	for k,_ := range ulMap {
		if _, ok := dlMap[k]; !ok {
			// If you fail here, one map lacks something.
			c.Fail()
		}
	}

	destFile, err := os.Open(path.Join(dlDir, fileName))
	c.Assert(err, chk.IsNil)

	// Assert downloaded data is consistent
	destBuffer :=  make([]byte, count)
	n, err := destFile.Read(destBuffer)
	c.Assert(err, chk.Equals, nil)
	c.Assert(n, chk.Equals, fileSize)
	c.Assert(destBuffer, chk.DeepEquals, fileData)
}

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
		AccountName: account,
		ContainerName: containerName,
		BlobName: fileName,
		SourcePath: filePath,
		Credential: credential,
		Parallelism: uint16(parallelism),
		BlockSize: int64(blockSize),
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

	// invoke restore to download the file back
	count, err = Restore(RestoreOptions{
		AccountName: account,
		ContainerName: containerName,
		BlobName: fileName,
		DestinationPath: destFilePath,
		Credential: credential,
		Parallelism: uint16(parallelism),
		BlockSize: int64(blockSize),
	})

	// Assert download was successful
	c.Assert(err, chk.IsNil)
	c.Assert(count, chk.Equals, int64(fileSize))

	// Assert downloaded data is consistent
	destBuffer :=  make([]byte, count)
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

func (s *cmdIntegrationSuite) TestUploadAndDownloadSymlinkFile(c *chk.C) {
	fileSize := 1024
	blockSize := 2048
	parallelism := 3
	// Test with a large-ish amount of symlinks jumping between sub and same directories.
	performUploadAndDownloadSymlinkTest(c, 8, fileSize, blockSize, parallelism)
}