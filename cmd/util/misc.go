// Copyright © 2020 Microsoft <wastore@microsoft.com>
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

package util

import (
	"context"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

func NewPipeline(ctx context.Context, c azblob.Credential, p Pacer, o azblob.PipelineOptions) pipeline.Pipeline {
	const tryTimeout = time.Minute * 15
	const retryDelay = time.Second * 1
	const maxRetryDelay = time.Second * 6
	const maxTries = 20

	r := azblob.RetryOptions{
		Policy:        0,
		MaxTries:      20,
		TryTimeout:    tryTimeout,
		RetryDelay:    retryDelay,
		MaxRetryDelay: maxRetryDelay,
	}
	// Closest to API goes first; closest to the wire goes last
	var f []pipeline.Factory

	if p != nil {
		f = append(f, NewRateLimiterPolicy(ctx, p))
	}
	f = append(f,
		azblob.NewTelemetryPolicyFactory(o.Telemetry),
		azblob.NewUniqueRequestIDPolicyFactory(),
		azblob.NewRetryPolicyFactory(r),
		c,
		azblob.NewRequestLogPolicyFactory(o.RequestLog),
		pipeline.MethodFactoryMarker()) // indicates at what stage in the pipeline the method factory is invoked

	return pipeline.NewPipeline(f, pipeline.Options{HTTPSender: o.HTTPSender, Log: o.Log})
}
