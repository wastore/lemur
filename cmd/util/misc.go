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
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	kvauth "github.com/Azure/azure-sdk-for-go/services/keyvault/auth"
	"github.com/Azure/azure-sdk-for-go/services/keyvault/v7.0/keyvault"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
)


type ErrorEx struct {
	code int32
	msg  string
}

func (e ErrorEx) ErrorCode() int32 {
	return e.code
}

func (e ErrorEx) Error() string {
	return e.msg
}

func ShouldRetry(err error) bool {
	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.Response().StatusCode == http.StatusForbidden {
			return true
		}
	}

	if errEx, ok := err.(ErrorEx); ok {
		if errEx.ErrorCode() == http.StatusForbidden {
			return true
		}
	}

	return false
}

func ShouldRefreshCreds(err error) bool {
	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.Response().StatusCode == http.StatusForbidden {
			return true
		}
	}

	if errEx, ok := err.(ErrorEx); ok {
		if errEx.ErrorCode() == http.StatusForbidden {
			return true
		}
	}

	return false
}

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
func GetKVSecret(kvURL, kvSecretName string) (secret string, err error) {
	authorizer, err := kvauth.NewAuthorizerFromEnvironment()
	if err != nil {
		return "", err
	}

	basicClient := keyvault.New()
	basicClient.Authorizer = authorizer

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Minute)
	secretResp, err := basicClient.GetSecret(ctx, kvURL, kvSecretName, "")
	if err != nil {
		return "", err
	}

	secret = *secretResp.Value
	if secret[0] != '?' {
		secret = "?" + secret
	}

	return secret, nil
}

func IsSASValid(sas string) (ok bool, reason string) {
	if sas == "" {
		return false, "Empty string returned."
	}

	if sas[0] == '?' {
		sas = sas[1:]
	}
	q, _ := url.ParseQuery(sas)
	if q.Get("sig") == "" {
		return false,"Missing signature"
	}
	if endTime := q.Get("se"); endTime == "" {
		return false,"Missing endTime"
	} else {
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			return false,"Invalid expiry time on SAS."+err.Error()
		}
		if t.Before(time.Now()) {
			return false, "Expired SAS returned"
		}
	}

	if v := os.Getenv("COPYTOOL_IGNORE_SAS_PERMISSIONS"); v != "" {
		ignore, err := strconv.ParseBool(v)
		if err == nil && ignore{
			return true, ""
		}
	}

	if signedPermissions := q.Get("sp"); signedPermissions == "" {
		return false, "Missing permissions"
	} else {
		// we need (r)ead - (w)rite - (l)ist permissions the minimum
		if !strings.ContainsRune(signedPermissions, 'r') ||
			!strings.ContainsRune(signedPermissions, 'w') ||
			!strings.ContainsRune(signedPermissions, 'l') {
			return false, "Insufficient permissions"
		}
	}
	return true, ""
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

func UnixError(err error) (ret int32) {
	if err == nil {
		ret = int32(0)
	}

	if stgErr, ok := err.(azblob.StorageError); ok {
		ret =  int32(stgErr.Response().StatusCode)
	} else if stgErr, ok := err.(azbfs.StorageError); ok {
		ret =  int32(stgErr.Response().StatusCode)
	} else if errEx, ok := err.(ErrorEx); ok {
		ret = errEx.ErrorCode()
	} else {
		ret = int32(syscall.EINVAL)
	}

	Log(pipeline.LogError, fmt.Sprintf("For error %v, returned status %d", err, ret))

	return ret
}

var jobMgr ste.IJobMgr
var globalPartNum uint32
var partNumLock sync.Mutex

func JobMgr() ste.IJobMgr {
	return jobMgr
}

func SetJobMgr(jm ste.IJobMgr) {
	jobMgr = jm
}

func NextPartNum() uint32 {
	partNumLock.Lock()
	defer partNumLock.Unlock()
	if globalPartNum == math.MaxUint32 {
		jobMgr.Reset(context.Background(), "Lustre")
		globalPartNum = 0
	}
	ret := globalPartNum
	globalPartNum = globalPartNum + 1
	return ret
}

func ResetPartNum() {
	partNumLock.Lock()
	defer partNumLock.Unlock()
	globalPartNum = 0
}

func Upload(ctx context.Context, filePath string, blobPath string, blockSize int64, meta azblob.Metadata) error {
	srcResource, _ := cmd.SplitResourceString(filePath, common.ELocation.Local())
	dstResource, _ := cmd.SplitResourceString(blobPath, common.ELocation.Blob())
	p := common.PartNumber(NextPartNum())

	fi, _ := os.Stat(filePath)

	t := common.CopyTransfer{
		Source:           "",
		Destination:      "",
		EntityType:       common.EEntityType.File(),
		LastModifiedTime: fi.ModTime(),
		SourceSize:       fi.Size(),
		Metadata:         common.FromAzBlobMetadataToCommonMetadata(meta),
	}

	var metadata = ""
	for k, v := range meta {
		metadata = metadata + fmt.Sprintf("%s=%s;", k, v)
	}
	if len(metadata) > 0 { //Remove trailing ';'
		metadata = metadata[:len(metadata)-1]
	}

	order := common.CopyJobPartOrderRequest{
		JobID:           JobMgr().JobID(),
		PartNum:         p,
		FromTo:          common.EFromTo.LocalBlob(),
		ForceWrite:      common.EOverwriteOption.True(),
		ForceIfReadOnly: false,
		AutoDecompress:  false,
		Priority:        common.EJobPriority.Normal(),
		LogLevel:        common.ELogLevel.Debug(),
		BlobAttributes: common.BlobTransferAttributes{
			BlobType:         common.EBlobType.BlockBlob(),
			BlockSizeInBytes: GetBlockSize(fi.Size(), blockSize),
			Metadata:         metadata,
		},
		CommandString:   "NONE",
		DestinationRoot: dstResource,
		SourceRoot:      srcResource,
		Fpo:             common.EFolderPropertiesOption.NoFolders(),
	}
	order.Transfers.List = append(order.Transfers.List, t)

	jppfn := ste.JobPartPlanFileName(fmt.Sprintf(ste.JobPartPlanFileNameFormat, jobMgr.JobID().String(), p, ste.DataSchemaVersion))
	jppfn.Create(order)

	waitForCompletion := make(chan struct{})
	jpm := jobMgr.AddJobPart(order.PartNum, jppfn, nil, order.SourceRoot.SAS, order.DestinationRoot.SAS, true, waitForCompletion)

	// Update jobPart Status with the status Manager
	jobMgr.SendJobPartCreatedMsg(ste.JobPartCreatedMsg{TotalTransfers: uint32(len(order.Transfers.List)),
		IsFinalPart:          true,
		TotalBytesEnumerated: order.Transfers.TotalSizeInBytes,
		FileTransfers:        order.Transfers.FileTransferCount,
		FolderTransfer:       order.Transfers.FolderTransferCount})

	canceled := false
	select {
	case <- ctx.Done():
		canceled = true
		jpm.Cancel()
		<-waitForCompletion
	case <-waitForCompletion:
	}

	
	part, _ := jobMgr.JobPartMgr(p)
	plan := part.Plan()
	status := plan.JobPartStatus()
	jpp := part.Plan().Transfer(0)
	errCode := jpp.ErrorCode()

	if p != 0 {
		jpm.Close()
	}

	if err := os.Remove(jppfn.GetJobPartPlanPath()); err != nil && p != 0 {
		Log(pipeline.LogError, err.Error())
	}

	if canceled {
		return ErrorEx{code: int32(syscall.ECANCELED), msg: "Job Cancelled"}
	}
	if status != common.EJobStatus.Completed() {
		return ErrorEx{code: errCode, msg: "STE Failed"}
	}

	return nil
}

func Download(ctx context.Context, blobPath string, filePath string, blockSize int64) error {
	dstResource, _ := cmd.SplitResourceString(filePath, common.ELocation.Local())
	srcResource, _ := cmd.SplitResourceString(blobPath, common.ELocation.Blob())
	p := common.PartNumber(NextPartNum())

	getBlobProperties := func(blobPath string) (*azblob.BlobGetPropertiesResponse, error) {
		rawURL, _ := url.Parse(blobPath)
		blobUrlParts := azblob.NewBlobURLParts(*rawURL)
		blobUrlParts.BlobName = strings.TrimSuffix(blobUrlParts.BlobName, "/")

		// perform the check
		blobURL := azblob.NewBlobURL(blobUrlParts.URL(), azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
		return blobURL.GetProperties(context.TODO(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	}

	props, err := getBlobProperties(blobPath)
	if err != nil {
		return err
	}
	t := common.CopyTransfer{
		Source:             "",
		Destination:        "",
		EntityType:         common.EEntityType.File(),
		LastModifiedTime:   props.LastModified(),
		SourceSize:         props.ContentLength(),
		ContentType:        props.ContentType(),
		ContentEncoding:    props.ContentEncoding(),
		ContentDisposition: props.ContentDisposition(),
		ContentLanguage:    props.ContentLanguage(),
		CacheControl:       props.CacheControl(),
		ContentMD5:         props.ContentMD5(),
		Metadata:           nil,
		BlobType:           props.BlobType(),
		BlobTags:           nil,
	}

	order := common.CopyJobPartOrderRequest{
		JobID:           JobMgr().JobID(),
		PartNum:         p,
		FromTo:          common.EFromTo.BlobLocal(),
		ForceWrite:      common.EOverwriteOption.True(),
		ForceIfReadOnly: false,
		AutoDecompress:  false,
		Priority:        common.EJobPriority.Normal(),
		LogLevel:        common.ELogLevel.Debug(),
		BlobAttributes: common.BlobTransferAttributes{
			BlobType:         common.EBlobType.BlockBlob(),
			BlockSizeInBytes: GetBlockSize(props.ContentLength(), blockSize),
		},
		CommandString:   "NONE",
		DestinationRoot: dstResource,
		SourceRoot:      srcResource,
		Fpo:             common.EFolderPropertiesOption.NoFolders(),
	}
	order.Transfers.List = append(order.Transfers.List, t)

	jppfn := ste.JobPartPlanFileName(fmt.Sprintf(ste.JobPartPlanFileNameFormat, jobMgr.JobID().String(), p, ste.DataSchemaVersion))
	jppfn.Create(order)

	waitForCompletion := make(chan struct{})
	jpm := jobMgr.AddJobPart(order.PartNum, jppfn, nil, order.SourceRoot.SAS, order.DestinationRoot.SAS, true, waitForCompletion)

	// Update jobPart Status with the status Manager
	jobMgr.SendJobPartCreatedMsg(ste.JobPartCreatedMsg{TotalTransfers: uint32(len(order.Transfers.List)),
		IsFinalPart:          true,
		TotalBytesEnumerated: order.Transfers.TotalSizeInBytes,
		FileTransfers:        order.Transfers.FileTransferCount,
		FolderTransfer:       order.Transfers.FolderTransferCount})
	
	part, _ := jobMgr.JobPartMgr(p)

	canceled := false		
	select {
	case <- ctx.Done():
		canceled = true
		jpm.Cancel()
		<-waitForCompletion
	case <-waitForCompletion:
	}
	
	plan := part.Plan()
	status := plan.JobPartStatus()
	jpp := part.Plan().Transfer(0)
	errCode := jpp.ErrorCode()

	if p != 0 {
		part.Close()
	}

	if err := os.Remove(jppfn.GetJobPartPlanPath()); err != nil && p != 0 {
		Log(pipeline.LogError, err.Error())
	}

	if canceled {
		return ErrorEx{code: int32(syscall.ECANCELED), msg: "Job Cancelled"}
	}
	if status != common.EJobStatus.Completed() {
		return ErrorEx{code: errCode, msg: "STE Failed"}
	}

	return nil
}