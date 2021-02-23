package lhsm_plugin_az_core

import (
	"net/http"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
	"github.com/Azure/azure-pipeline-go/pipeline"
	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestRestoreSmallBlob(c *chk.C) {
	fileSize := 1024
	blockSize := 2048
	parallelism := 3
	performRestoreTest(c, fileSize, blockSize, parallelism)
}

// test download with the source data uploaded directly to the service from memory
// this is an independent check that download works
func performRestoreTest(c *chk.C, fileSize, blockSize, parallelism int) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobURL, blobName := createNewBlockBlob(c, containerURL, "")

	// stage the source blob with small amount of data
	reader, srcData := getRandomDataAndReader(fileSize)
	_, err := blobURL.Upload(ctx, reader, azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{}, azblob.AccessTierNone)
	c.Assert(err, chk.IsNil)

	// set up destination file
	destination := filepath.Join(os.TempDir(), blobName)
	destFile, err := os.Create(destination)
	c.Assert(err, chk.Equals, nil)
	defer destFile.Close()
	defer os.Remove(destination)

	//setup logging
	util.InitJobLogger(pipeline.LogDebug)

	// exercise restore
	account, key := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(account, key)
	c.Assert(err, chk.IsNil)
	blobName = containerName + "/" + blobName
	count, err := Restore(RestoreOptions{
		AccountName:     account,
		ContainerName:   "",
		BlobName:        blobName,
		DestinationPath: destination,
		Credential:      credential,
		Parallelism:     uint16(parallelism),
		BlockSize:       int64(blockSize),
		HTTPClient: &http.Client{},
	})

	// make sure we got the right info back
	c.Assert(err, chk.IsNil)
	c.Assert(count, chk.Equals, int64(len(srcData)))

	// Assert downloaded data is consistent
	destBuffer := make([]byte, count)
	n, err := destFile.Read(destBuffer)
	c.Assert(err, chk.Equals, nil)
	c.Assert(n, chk.Equals, len(srcData))
	c.Assert(destBuffer, chk.DeepEquals, srcData)
}
