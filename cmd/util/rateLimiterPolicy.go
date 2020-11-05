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
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

func logThroughput(ctx context.Context, p Pacer) {
	interval := 4 * time.Second
	intervalStartTime := time.Now()
	prevBytesTransferred := p.GetTotalTraffic()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			bytesOnWireMb := float64(float64(p.GetTotalTraffic()-prevBytesTransferred) / (1000 * 1000))
			timeElapsed := time.Since(intervalStartTime).Seconds()
			if timeElapsed != 0 {
				throughput := bytesOnWireMb / float64(timeElapsed)
				Log(pipeline.LogInfo, fmt.Sprintf("4-sec throughput: %v MBPS", throughput))
			}
			// reset the interval timer and byte count
			intervalStartTime = time.Now()
			prevBytesTransferred = p.GetTotalTraffic()
		}
	}
}

type rateLimiterPolicy struct {
	next  pipeline.Policy
	pacer Pacer
}

func (r *rateLimiterPolicy) Do(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
	err := r.pacer.RequestTrafficAllocation(ctx, request.ContentLength)
	if err != nil {
		return nil, errors.New("failed to pace request block")
	}

	resp, err := r.next.Do(ctx, request)

	err = r.pacer.RequestTrafficAllocation(ctx, resp.Response().ContentLength)
	if err != nil {
		return nil, errors.New("failed to pace response block")
	}

	return resp, err
}

func NewRateLimiterPolicy(ctx context.Context, pacer Pacer) pipeline.Factory {
	go logThroughput(ctx, pacer)
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		r := rateLimiterPolicy{next: next, pacer: pacer}
		return r.Do
	})
}
