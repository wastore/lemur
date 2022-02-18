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
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	kvauth "github.com/Azure/azure-sdk-for-go/services/keyvault/auth"
	"github.com/Azure/azure-sdk-for-go/services/keyvault/v7.0/keyvault"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

//HTTPClientFactory returns http sender with given client
func HTTPClientFactory(client *http.Client) pipeline.FactoryFunc {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			r, err := client.Do(request.WithContext(ctx))
			if err != nil {
				err = pipeline.NewError(err, "HTTP request failed")
			}
			return pipeline.NewHTTPResponse(r), err
		}
	})
}

// NewPipeline creates a blobpipeline with these options
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

//GetKVSecret returns string secret by name 'kvSecretName' in keyvault 'kvName'
//Uses MSI auth to login
func GetKVSecret(kvName, kvSecretName string) (secret string, err error) {
	authorizer, err := kvauth.NewAuthorizerFromEnvironment()
	if err != nil {
		return "", err
	}

	basicClient := keyvault.New()
	basicClient.Authorizer = authorizer

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Minute)
	secretResp, err := basicClient.GetSecret(ctx, "https://"+kvName+".vault.azure.net", kvSecretName, "")
	if err != nil {
		return "", err
	}

	return *secretResp.Value, nil
}

func IsSASValid(sas string) bool {
	q, _ := url.ParseQuery(sas)

	if q.Get("sig") == "" {
		Log(pipeline.LogError, "Invalid SAS returned. Missing signature")
		return false
	}
	if endTime := q.Get("se"); endTime != "" {
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			Log(pipeline.LogError, "Invalid expiry time on SAS." + err.Error())
			return false
		}
		if t.Before(time.Now()) {
			Log(pipeline.LogError, "Expired SAS returned")
			return false
		}
	}
	return true
}

func GetBlockSize(filesize int64, minBlockSize int64) (blockSize int64) {
	blockSizeThreshold := int64(256 * 1024 * 1024) /* 256 MB */
	blockSize = minBlockSize

	/* We should not perform checks on filesize, block size limitation here. Those are performed in SDK
	 * and take care of themselves when limits change
	 */

	for ; uint32(filesize/blockSize) > azblob.BlockBlobMaxBlocks; blockSize = 2 * blockSize {
		if blockSize > blockSizeThreshold {
			/*
			 * For a RAM usage of 0.5G/core, we would have 4G memory on typical 8 core device, meaning at a blockSize of 256M,
			 * we can have 4 blocks in core, waiting for a disk or n/w operation. Any higher block size would *sort of*
			 * serialize n/w and disk operations, and is better avoided.
			 */
			blockSize = filesize / azblob.BlockBlobMaxBlocks
			break
			}
	}

	return blockSize
}