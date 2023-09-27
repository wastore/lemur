// Copyright Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
package copier

import (
	"bytes"
	"context"
  "fmt"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/google/uuid"
)

const (
    maxUploadBlobBytes = 5000 * 1024 * 1024
)

type nopCloser struct {
	io.ReadSeeker
}

func (nopCloser) Close() error { return nil }

func withNopCloser(r io.ReadSeeker) io.ReadSeekCloser {
	return nopCloser{r}
}

func getUploadOptions(o *blockblob.UploadFileOptions) *blockblob.UploadOptions {
	return &blockblob.UploadOptions{
		Tags:             o.Tags,
		Metadata:         o.Metadata,
		Tier:             o.AccessTier,
		HTTPHeaders:      o.HTTPHeaders,
		CPKInfo:          o.CPKInfo,
		CPKScopeInfo:     o.CPKScopeInfo,
		AccessConditions: o.AccessConditions,
	}
}

func getStageBlockOptions(o *blockblob.UploadFileOptions) *blockblob.StageBlockOptions {
	var accessConditions *blob.LeaseAccessConditions
	if o.AccessConditions != nil {
		accessConditions = o.AccessConditions.LeaseAccessConditions
	}
	return &blockblob.StageBlockOptions{
		CPKInfo:               o.CPKInfo,
		CPKScopeInfo:          o.CPKScopeInfo,
		LeaseAccessConditions: accessConditions,
	}
}

func getCommitBlockListOptions(o *blockblob.UploadFileOptions) *blockblob.CommitBlockListOptions {
	return &blockblob.CommitBlockListOptions{
		Tags:             o.Tags,
		Metadata:         o.Metadata,
		Tier:             o.AccessTier,
		HTTPHeaders:      o.HTTPHeaders,
		CPKInfo:          o.CPKInfo,
		CPKScopeInfo:     o.CPKScopeInfo,
		AccessConditions: o.AccessConditions,
	}
}

/*
 * Upload file will upload filepath to blob pointed by b.
 * only [Blocksize, Tags, Metadata, AccessTier, HTTPHeaders, CPKInfo, CPKScopeInfo, AccessConditions]
 * fields in UploadOptions are supported.
 */
func (c *copier) UploadFile(ctx context.Context,
	b *blockblob.Client,
	filepath string,
	o *blockblob.UploadFileOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if o == nil {
		o = &blockblob.UploadFileOptions{}
	}
	go c.monitorContext(ctx, cancel)

	// 1. Calculate the size of the destination file
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()
  fmt.Print("ELLIS: (%s) size (%ld)", filepath, fileSize)

	// get default blocksize if not specified, or revise blocksize if too small
	// relative to the file to result in a commit beneath blockblob.MaxBlocks
	o.BlockSize, err = getBlockSize(o.BlockSize, fileSize)
	if err != nil {
		return err
	}
  fmt.Print("ELLIS: (%s) blocksize (%ld)", filepath, o.BlockSize)

	if (o.BlockSize >= fileSize && fileSize <= maxUploadBlobBytes) { //perform a single thread copy here.
		_, err := b.Upload(ctx, newPacedReadSeekCloser(ctx, c.pacer, file), getUploadOptions(o))
		return err
	}

	return c.uploadInternal(ctx, cancel, b, file, fileSize, o)
}

func (c *copier) uploadInternal(ctx context.Context,
	cancel context.CancelFunc,
	b *blockblob.Client,
	file io.ReadSeekCloser,
	fileSize int64,
	o *blockblob.UploadFileOptions) error {
	// short hand for routines to report and error
	errorChannel := make(chan error)
	postError := func(err error) {
		select {
		case <-ctx.Done():
		case errorChannel <- err:
		}
	}

	numBlocks := uint16(((fileSize - 1) / o.BlockSize) + 1)
	var wg sync.WaitGroup

	blockNames := make([]string, numBlocks)

	uploadBlock := func(buff []byte, blockIndex uint16) {
		defer wg.Done()
    fmt.Print("ELLIS: multithread: blockIndex=%d", blockIndex)
		body := newPacedReadSeekCloser(ctx, c.pacer, withNopCloser(bytes.NewReader(buff)))
		blockName := base64.StdEncoding.EncodeToString([]byte(uuid.New().String()))
		blockNames[blockIndex] = blockName

		_, err := b.StageBlock(ctx, blockNames[blockIndex], body, getStageBlockOptions(o))
		if err != nil {
			postError(err)
		}

		//Return the buffer
		c.slicePool.ReturnSlice(buff)
		c.cacheLimiter.Remove(int64(len(buff)))
		if o.Progress != nil {
			o.Progress(int64(len(buff)))
		}
	}

	var err error
	go func() {
		// This goroutine will monitor error channel and
		// cancel the context if any block reports error
		err = <-errorChannel
		cancel()
	}()

	for blockNum := uint16(0); blockNum < numBlocks; blockNum++ {
		currBlockSize := o.BlockSize
		if blockNum == numBlocks-1 { // Last block
			// Remove size of all transferred blocks from total
			currBlockSize = fileSize - (int64(blockNum) * o.BlockSize)
		}

		if err := c.cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			postError(err)
			break
		}
		buff := c.slicePool.RentSlice(currBlockSize)

		n, err := file.Read(buff)
		if err != nil {
			postError(err)
			break
		}
		if n != int(currBlockSize) {
			postError(errors.New("invalid read"))
			break
		}

		f := func(buff []byte, blockNum uint16) func() {
			return func() { uploadBlock(buff, blockNum) }
		}(buff, blockNum)

		wg.Add(1)
		c.execute(f)
	}

	// Wait for all chunks to be done.
	wg.Wait()
	if err != nil {
		return err
	}

	_, err = b.CommitBlockList(ctx, blockNames, getCommitBlockListOptions(o))

	return err
}
