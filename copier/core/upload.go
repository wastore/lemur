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
	"encoding/base64"
	"io"
	"os"
	"sync"

	"github.com/intel-hpdd/logging/debug"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/google/uuid"
	"github.com/wastore/lemur/cmd/util"
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
	blobPath string,
	o *blockblob.UploadFileOptions,
	getNewStorageClientsCb NewStorageClientsCb) error {
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

	// get default blocksize if not specified, or revise blocksize if too small
	// relative to the file to result in a commit beneath blockblob.MaxBlocks
	o.BlockSize, err = getBlockSize(o.BlockSize, fileSize)
	if err != nil {
		return err
	}

	if o.BlockSize >= fileSize && fileSize <= maxUploadBlobBytes { //perform a single thread copy here.
		_, err := b.Upload(ctx, newPacedReadSeekCloser(ctx, c.pacer, file), getUploadOptions(o))
		if util.ShouldRefreshCreds(err) {
			_, b, err = getNewStorageClientsCb(blobPath)
			if err != nil {
				return err
			}
			_, err = b.Upload(ctx, newPacedReadSeekCloser(ctx, c.pacer, file), getUploadOptions(o))

		}
		return err
	}

	return c.uploadInternal(ctx, cancel, b, file, fileSize, blobPath, o, getNewStorageClientsCb)
}

func (c *copier) uploadInternal(ctx context.Context,
	cancel context.CancelFunc,
	b *blockblob.Client,
	file io.ReadSeekCloser,
	fileSize int64,
	blobPath string,
	o *blockblob.UploadFileOptions,
	getNewStorageClientsCb NewStorageClientsCb) error {
	// short hand for routines to report and error

	errorChannel := make(chan error)
	setErrorIfNotCancelled := func(err error) {
		select {
		case <-ctx.Done():
		case errorChannel <- err:
		}
	}

	numBlocks := uint16(((fileSize - 1) / o.BlockSize) + 1)
	var wg sync.WaitGroup

	blockNames := make([]string, numBlocks)

	debug.Printf("Num Blocks: %v Block Size: %v\n", numBlocks, o.BlockSize)
	uploadBlock := func(buff []byte, blockIndex uint16) {
		defer wg.Done()
		if ctx.Err() != nil {
			return
		}
		body := newPacedReadSeekCloser(ctx, c.pacer, withNopCloser(bytes.NewReader(buff)))
		blockName := base64.StdEncoding.EncodeToString([]byte(uuid.New().String()))
		blockNames[blockIndex] = blockName

		_, err := b.StageBlock(ctx, blockNames[blockIndex], body, getStageBlockOptions(o))
		if util.ShouldRefreshCreds(err) {
			_, b, err = getNewStorageClientsCb(blobPath)
			if err != nil {
				debug.Printf("Failed to get new storage clients: %v\n", err)
				setErrorIfNotCancelled(err)
				return
			}
			// Need to rewind the body in order to restart staging the block
			_, err = body.Seek(0, io.SeekStart)
			if err != nil {
				debug.Printf("Failed to seek to start of body: %v\n", err)
				setErrorIfNotCancelled(err)
				return
			}
			_, err = b.StageBlock(ctx, blockNames[blockIndex], body, getStageBlockOptions(o))
			if err != nil {
				debug.Printf("Failed to stage block %v: %v", blockIndex, err)
			}
		}
		if err != nil {
			setErrorIfNotCancelled(err)
			return
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

	debug.Printf("Block Size: %v Block Count: %v\n", o.BlockSize, numBlocks)
	// NOTE: Use of uint16 here is fine as the maximum number of blocks in a block blob
	// is 50,000 and the copier picks a chunk size to stay below this value.
	for blockNum := uint16(0); blockNum < numBlocks; blockNum++ {
		if ctx.Err() != nil { // If the context is close, do not schedule any more.
			break
		}
		currBlockSize := o.BlockSize
		if blockNum == numBlocks-1 { // Last block
			// Remove size of all transferred blocks from total
			currBlockSize = fileSize - (int64(blockNum) * o.BlockSize)
		}

		if err := c.cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			setErrorIfNotCancelled(err)
			break
		}
		buff := c.slicePool.RentSlice(currBlockSize)

		// io.ReadFull will read from the given io.Reader until the given
		// buffer is full. Since the buffer is sized to match the block
		// size or be exactly the remaining number of bytes, io.EOF is
		// not handled.
		if _, err = io.ReadFull(file, buff); err != nil {
			setErrorIfNotCancelled(err)
			break
		}

		f := func(buff []byte, blockNum uint16) func() {
			return func() { uploadBlock(buff, blockNum) }
		}(buff, blockNum)

		wg.Add(1)
		//schedule the block
		if err := c.execute(f); err != nil {
			setErrorIfNotCancelled(err)
			break
		}
	}

	// Wait for all scheduled chunks to be done.
	wg.Wait()
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	_, err = b.CommitBlockList(ctx, blockNames, getCommitBlockListOptions(o))
	if util.ShouldRefreshCreds(err) {
		_, b, err = getNewStorageClientsCb(blobPath)
		if err != nil {
			return err
		}
		_, err = b.CommitBlockList(ctx, blockNames, getCommitBlockListOptions(o))
	}
	return err
}
