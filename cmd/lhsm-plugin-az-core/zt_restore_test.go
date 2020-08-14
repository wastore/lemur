package lhsm_plugin_az_core

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
	"os"
	"path/filepath"
	"strings"
)

func (s *cmdIntegrationSuite) TestRestoreSmallBlob(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobURL, blobName := createNewBlockBlob(c, containerURL, "")

	// stage the source blob with small amount of data
	_, err := blobURL.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{})
	c.Assert(err, chk.IsNil)

	// set up destination file
	destination := filepath.Join(os.TempDir(), blobName)
	destFile, err := os.Create(destination)
	c.Assert(err, chk.Equals, nil)
	defer destFile.Close()
	defer os.Remove(destination)

	// exercise restore
	account, key := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(account, key)
	c.Assert(err, chk.IsNil)
	count, err := Restore(RestoreOptions{
		AccountName: account,
		ContainerName: containerName,
		BlobName: blobName,
		DestinationPath: destination,
		Credential: credential,
		Parallelism: 16,
		BlockSize: 5*1024,
	})

	// make sure we got the right info back
	c.Assert(err, chk.IsNil)
	c.Assert(count, chk.Equals, int64(len(blockBlobDefaultData)))

	// Assert downloaded data is consistent
	destBuffer :=  make([]byte, count)
	n, err := destFile.Read(destBuffer)
	c.Assert(err, chk.Equals, nil)
	c.Assert(n, chk.Equals, len(blockBlobDefaultData))
	c.Assert(string(destBuffer), chk.DeepEquals, blockBlobDefaultData)
}