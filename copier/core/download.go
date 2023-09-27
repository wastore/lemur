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
  "fmt"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
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
	filepath string,
	o *blob.DownloadFileOptions) (int64, error) {

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
  fmt.Print("ELLIS: (%s) size (%ld)", filepath, size)

	// get default blocksize if not specified, or revise blocksize if too small
	// relative to the file to result in a commit beneath blockblob.MaxBlocks
	o.BlockSize, err = getBlockSize(o.BlockSize, size)
	if err != nil {
		return 0, err
	}
  fmt.Print("ELLIS: (%s) blocksize (%ld)", filepath, o.BlockSize)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()
  fmt.Print("ELLIS: (%s) file opened", filepath)

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
  fmt.Print("ELLIS: (%s) file stat'd", filepath)

	// Nothing to be done
	if size == 0 {
		return 0, nil
	}

	if (o.BlockSize >= size) { //perform a single thread copy here.
    fmt.Print("ELLIS: (%s) single thread copy", filepath)
		dr, err := b.DownloadStream(ctx, downloadFileOptionsToStreamOptions(o))
		if err != nil {
			return 0, err
		}
		var body io.ReadCloser = dr.NewRetryReader(ctx, &o.RetryReaderOptionsPerBlock)
		defer body.Close()

		return file.ReadFrom(newPacedReader(ctx, c.pacer, body))
	}

  fmt.Print("ELLIS: (%s) multi thread copy", filepath)
	return c.downloadInternal(ctx, cancel, b, file, size, o)
}

func (c *copier) downloadInternal(
	ctx context.Context,
	cancel context.CancelFunc,
	b *blob.Client,
	file *os.File,
	fileSize int64,
	o *blob.DownloadFileOptions) (int64, error) {
	// short hand for routines to report and error
	errorChannel := make(chan error)
	postError := func(err error) {
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
  fmt.Print("ELLIS: numBlocks=%d, fileSize=%d, blocksize=%d", numBlocks, count, o.BlockSize)

	blocks := make([]chan []byte, numBlocks)
	for i := range blocks {
		blocks[i] = make(chan []byte)
	}

	totalWrite := int64(0)
	wg.Add(1) // for the writer below
	go func() {
		defer wg.Done()
		for _, block := range blocks {
			select {
			case <-ctx.Done():
        fmt.Print("ELLIS: multithread write context done")
				return
			case buff := <-block:
				n, err := file.Write(buff)
				if err != nil {
					postError(err)
          fmt.Print("ELLIS: multithread write failed 158")
					return
				}
				if n != len(buff) {
					postError(io.ErrShortWrite)
          fmt.Print("ELLIS: multithread write failed 163")
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
			postError(io.ErrShortWrite)
		}
		file.Sync()
	}()

	// DownloadBlock func downloads each block of the blob into buffer provided
	downloadBlock := func(buff []byte, blockNum uint16, currentBlockSize, offset int64) {
		defer wg.Done()

    fmt.Print("ELLIS: multithread write: blockNum=%d, curBlockSize=%d, offset=%d", blockNum, currentBlockSize, offset)
		options := downloadFileOptionsToStreamOptions(o)
		options.Range = blob.HTTPRange{Offset: offset, Count: currentBlockSize}
		dr, err := b.DownloadStream(ctx, options)

		if err != nil {
			postError(err)
      fmt.Print("ELLIS: multithread write failed 194")
			return
		}

		var body io.ReadCloser = dr.NewRetryReader(ctx, &o.RetryReaderOptionsPerBlock)
		defer body.Close()

		if err := c.pacer.RequestTrafficAllocation(ctx, int64(len(buff))); err != nil {
			postError(err)
      fmt.Print("ELLIS: multithread write failed 202")
			return
		}

		_, err = io.ReadFull(body, buff)
		if err != nil {
			postError(err)
      fmt.Print("ELLIS: multithread write failed 209")
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
		currBlockSize := o.BlockSize
		if blockNum == numBlocks-1 { // Last block
			// Remove size of all transferred blocks from total
			currBlockSize = count - (int64(blockNum) * o.BlockSize)
		}

		offset := int64(blockNum) * o.BlockSize

		// allocate a buffer. This buffer will be released by the fileWriter
		if err := c.cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			postError(err)
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

	// Wait for all chunks to be done.
	wg.Wait()
	if err != nil {
    fmt.Print("ELLIS: multithread write failed 255")
		return 0, err
	}

  fmt.Print("ELLIS: multithread write success")
	return count, nil
}
