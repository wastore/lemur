package lhsm_plugin_az_core

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	copier "github.com/wastore/lemur/copier/core"
	"github.com/wastore/lemur/cmd/util"
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
	copier := copier.NewCopier(0, maxBlockLength, defaultCachelimit, defaultConcurrency)
	bsu := getBSU()
	containerURL, cName := createNewContainer(c, bsu)
	fmt.Println(containerURL.URL())
	defer bsu.DeleteContainer(context.TODO(), cName, nil)
	blobName := generateBlobName()

	blobURL := containerURL.NewBlockBlobClient(blobName)
	// stage the source blob with small amount of data
	reader, srcData := getRandomDataAndReader(fileSize)
	_, err := blobURL.UploadStream(ctx, reader, nil)
	c.Assert(err, chk.IsNil)

	// set up destination file
	destination := filepath.Join(os.TempDir(), blobName)
	destFile, err := os.Create(destination)
	c.Assert(err, chk.Equals, nil)
	defer destFile.Close()
	defer os.Remove(destination)

	//setup logging
	util.InitJobLogger(pipeline.LogDebug)

	count, err := Restore(context.TODO(), copier,
		RestoreOptions{
			ContainerURL:    containerURL,
			BlobName:        blobName,
			DestinationPath: destination,
			BlockSize:       int64(blockSize),
			HTTPClient:      &http.Client{},
			OpStartTime:     time.Now(),
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
