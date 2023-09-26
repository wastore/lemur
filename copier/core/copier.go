// Copyright © Microsoft <wastore@microsoft.com>
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
	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

var ErrCopierClosed = errors.New("copier closed")

type Copier interface {
	DownloadFile(ctx context.Context, bb *blockblob.Client, filepath string, o *blob.DownloadFileOptions) (int64, error)
	UploadFile(ctx context.Context, b *blockblob.Client, filepath string, o *blockblob.UploadFileOptions) error
	Close()
}

type copier struct {
	ctx          context.Context
	pacer        iPacerAdmin
	slicePool    iByteSlicePooler
	cacheLimiter iCacheLimiter
	opChan       chan func()
	closer       context.CancelFunc
}

func (c *copier) Close() {
	c.closer()
	close(c.opChan)
}

// Execute will dispach f to be run if Copier is alive
func (c *copier) execute(f func()) error {
	select {
	case <-c.ctx.Done():
		return ErrCopierClosed
	case c.opChan <- f:
		return nil
	}
}

// monitor context will cancel ctx if copier was cancelled.
func (c *copier) monitorContext(ctx context.Context, cancel context.CancelFunc) {
	select {
	case <-c.ctx.Done():
		cancel()
	case <-ctx.Done():
	}
}

func NewCopier(throughputBytesPerSec int64,
	maxSliceLength int64,
	memLimit int64,
	concurrency int) Copier {

	pacer := NewTokenBucketPacer(throughputBytesPerSec, int64(0))
	slicePool := newMultiSizeSlicePool(maxSliceLength)
	cachelimiter := newCacheLimiter(memLimit)
	opChan := make(chan func())

	ctx, cancel := context.WithCancel(context.TODO())

	c := &copier{
		ctx:          ctx,
		pacer:        pacer,
		slicePool:    slicePool,
		cacheLimiter: cachelimiter,
		opChan:       opChan,
		closer:       cancel,
	}

	worker := func() {
		for f := range c.opChan {
			f()
		}
	}

	// start workers
	for i := 0; i < concurrency; i++ {
		go worker()
	}

	return c
}
