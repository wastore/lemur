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
)

type pacedReader struct {
	pacer
	r   io.Reader
	ctx context.Context
}

func (pr *pacedReader) Read(p []byte) (n int, err error) {
	//pr.RequestTrafficAllocation(pr.ctx, int64(len(p)))
	return pr.r.Read(p)
}

func newPacedReader(ctx context.Context, p pacer, r io.Reader) io.Reader {
	if p == nil {
		return r
	}

	return &pacedReader{p, r, ctx}
}

//=============================================================================

type pacedReadSeekCloser struct {
	pacer
	r   io.ReadSeekCloser
	ctx context.Context
}

func (pr *pacedReadSeekCloser) Read(p []byte) (int, error) {
	pr.RequestTrafficAllocation(pr.ctx, int64(len(p)))
	return pr.r.Read(p)
}

func (pr *pacedReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return pr.r.Seek(offset, whence)
}

func (pr *pacedReadSeekCloser) Close() error {
	return pr.r.Close()
}

func newPacedReadSeekCloser(ctx context.Context, p pacer, r io.ReadSeekCloser) io.ReadSeekCloser {
	if p == nil {
		return r
	}

	return &pacedReadSeekCloser{p, r, ctx}
}
