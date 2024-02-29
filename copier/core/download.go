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
	"context"
	"io"
	"os"
	"sync"

	"github.com/intel-hpdd/logging/debug"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/wastore/lemur/cmd/util"
)

func downloadFileOptionsToStreamOptions(f *blob.DownloadFileOptions) *blob.DownloadStreamOptions {
	return &blob.DownloadStreamOptions{
		AccessConditions: f.AccessConditions,
		CPKInfo:          f.CPKInfo,
		CPKScopeInfo:     f.CPKScopeInfo,
	}
}

/*
 * Downloads file pointed by bb to filepath.
 * Range and concurrency options are not supported.
 */
func (c *copier) DownloadFile(
	ctx context.Context,
	bb *blockblob.Client,
	blobPath string,
	filepath string,
	o *blob.DownloadFileOptions,
	getNewStorageClientsCb NewStorageClientsCb) (int64, error) {

	if o == nil {
		o = &blob.DownloadFileOptions{}
	}

	b := bb.BlobClient()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go c.monitorContext(ctx, cancel)

	// 1. Calculate the size of the destination file
	var size int64
	props, err := b.GetProperties(ctx, nil)
	if err != nil {
		return 0, err
	}
	size = *props.ContentLength

	// get default blocksize if not specified, or revise blocksize if too small
	// relative to the file to result in a commit beneath blockblob.MaxBlocks
	o.BlockSize, err = getBlockSize(o.BlockSize, size)
	if err != nil {
		return 0, err
	}

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// 2. Compare and try to resize local file's size if it doesn't match Azure blob's size.
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() != size {
		if err = file.Truncate(size); err != nil {
			return 0, err
		}
	}

	// Nothing to be done
	if size == 0 {
		return 0, nil
	}

	if (o.BlockSize >= size) { //perform a single thread copy here.
		dr, err := b.DownloadStream(ctx, downloadFileOptionsToStreamOptions(o))
		if util.ShouldRefreshCreds(err) {
			_, bb, err = getNewStorageClientsCb(blobPath)
			if err != nil {
				debug.Printf("Failed to get new storage clients: %v\n", err)
				return 0, err
			}
			b = bb.BlobClient()
			dr, err = b.DownloadStream(ctx, downloadFileOptionsToStreamOptions(o))
			if err != nil {
				debug.Printf("Failed to get new download stream: %v\n", err)
			}
		}
		if err != nil {
			return 0, err
		}
		var body io.ReadCloser = dr.NewRetryReader(ctx, &o.RetryReaderOptionsPerBlock)
		defer body.Close()

		return file.ReadFrom(newPacedReader(ctx, c.pacer, body))
	}

	return c.downloadInternal(ctx, cancel, b, blobPath, file, size, o, getNewStorageClientsCb)
}

func (c *copier) downloadInternal(
	ctx context.Context,
	cancel context.CancelFunc,
	b *blob.Client,
	blobPath string,
	file *os.File,
	fileSize int64,
	o *blob.DownloadFileOptions,
	getNewStorageClientsCb NewStorageClientsCb) (int64, error) {
	// short hand for routines to report and error
	errorChannel := make(chan error)
	setErrorIfNotCancelled := func(err error) {
		select {
		case <-ctx.Done():
		case errorChannel <- err:
		}
	}

	// to synchronize all block scheduling and writer threads.
	var wg sync.WaitGroup

	// file serial writer
	count := fileSize
	numBlocks := uint16(((count - 1) / o.BlockSize) + 1)

	blocks := make([]chan []byte, numBlocks)
	for i := range blocks {
		blocks[i] = make(chan []byte)
	}

	debug.Printf("Block Size: %v Blocks: %v\n", o.BlockSize, numBlocks)
	// TODO: This is an odd way to do what could be multiple writers to Lustre
	// with pwrite. Each block must wait for the previous block to be fetched
	// from blob. Maybe this is to bound the CPU load?
	totalWrite := int64(0)
	wg.Add(1) // for the writer below
	go func() {
		defer wg.Done()
		for _, block := range blocks {
			select {
			case <-ctx.Done():
				return
			case buff := <-block:
				n, err := file.Write(buff)
				if err != nil {
					debug.Printf("File write error: %v\n", err)
					setErrorIfNotCancelled(err)
					return
				}
				if n != len(buff) {
					debug.Printf("Short Write error. Wrote: %v Expected: %v\n", n, len(buff))
					setErrorIfNotCancelled(io.ErrShortWrite)
					return
				}

				c.slicePool.ReturnSlice(buff)
				c.cacheLimiter.Remove(int64(len(buff)))
				totalWrite += int64(n)
				if o.Progress != nil {
					o.Progress(int64(n))
				}
			}
		}

		if totalWrite != count {
			debug.Printf("Total short write. Wrote: %v Expected: %v\n", totalWrite, count)
			setErrorIfNotCancelled(io.ErrShortWrite)
		}
		file.Sync()
	}()

	// DownloadBlock func downloads each block of the blob into buffer provided
	downloadBlock := func(buff []byte, blockNum uint16, currentBlockSize, offset int64) {
		defer wg.Done()
		if ctx.Err() != nil {
			return
		}

		options := downloadFileOptionsToStreamOptions(o)
		options.Range = blob.HTTPRange{Offset: offset, Count: currentBlockSize}
		dr, err := b.DownloadStream(ctx, options)
		if err != nil {
			debug.Printf("Download Stream Error: %v\n", err)
		}
		if util.ShouldRefreshCreds(err) {
			// Need a second error variable here otherwise := will create an
			// error variable local to this scope and not overwrite the outer
			// err.
			//
			// The outer err is overwritten later in this scope.
			_, bb, err2 := getNewStorageClientsCb(blobPath)
			if err2 != nil {
				debug.Printf("Failed to get new storage clients: %v\n", err2)
				setErrorIfNotCancelled(err2)
				return
			}
			b = bb.BlobClient()
			dr, err = b.DownloadStream(ctx, options)
			if err != nil {
				debug.Printf("Failed to get new download stream: %v\n", err)
			}
		}
		if err != nil {
			setErrorIfNotCancelled(err)
			return
		}

		var body io.ReadCloser = dr.NewRetryReader(ctx, &o.RetryReaderOptionsPerBlock)
		defer body.Close()

		if err := c.pacer.RequestTrafficAllocation(ctx, int64(len(buff))); err != nil {
			debug.Printf("Request Traffic Allocation Error: %v\n", err)
			setErrorIfNotCancelled(err)
			return
		}

		_, err = io.ReadFull(body, buff)
		// Restart the whole read if we failed with expired credentials
		if util.ShouldRefreshCreds(err) {
			// Need a second error variable here otherwise := will create an
			// error variable local to this scope and not overwrite the outer
			// err.
			//
			// The outer err is overwritten later in this scope.
			_, bb, err2 := getNewStorageClientsCb(blobPath)
			if err2 != nil {
				debug.Printf("Failed to get new storage clients: %v\n", err2)
				setErrorIfNotCancelled(err2)
				return
			}
			b = bb.BlobClient()
			dr, err = b.DownloadStream(ctx, options)
			if err != nil {
				debug.Printf("Failed to get new download stream: %v\n", err)
				setErrorIfNotCancelled(err)
				return
			}
			body = dr.NewRetryReader(ctx, &o.RetryReaderOptionsPerBlock)
			_, err = io.ReadFull(body, buff)
			if err != nil {
				debug.Printf("Failed to read body from buff: %v\n", err)
			}
		}

		if err != nil {
			setErrorIfNotCancelled(err)
			return
		}

		// Send to the filewriter
		blocks[blockNum] <- buff
	}

	var err error
	go func() {
		// This goroutine will monitor above channel and
		// cancel the context if any block reports error
		err = <-errorChannel
		cancel()
	}()

	for blockNum := uint16(0); blockNum < numBlocks; blockNum++ {
		if ctx.Err() != nil {
			break
		}
		currBlockSize := o.BlockSize
		if blockNum == numBlocks-1 { // Last block
			// Remove size of all transferred blocks from total
			currBlockSize = count - (int64(blockNum) * o.BlockSize)
		}

		offset := int64(blockNum) * o.BlockSize

		// allocate a buffer. This buffer will be released by the fileWriter
		if err := c.cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			setErrorIfNotCancelled(err)
			break
		}
		buff := c.slicePool.RentSlice(currBlockSize)

		f := func(buff []byte, blockNum uint16, curBlockSize, offset int64) func() {
			return func() {
				downloadBlock(buff, blockNum, curBlockSize, offset)
			}
		}(buff, blockNum, currBlockSize, offset)

		// send
		wg.Add(1)
		c.execute(f)
	}

	// Wait for all scheduled chunks to be done.
	wg.Wait()
	if err != nil {
		return 0, err
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	return count, nil
}
