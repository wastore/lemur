// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package lhsm_plugin_az_core

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

const (
	MiB                   = 1024 * 1024
	throughputBytesPerSec = int64(0)
	maxBlockLength        = blockblob.MaxStageBlockBytes
	defaultConcurrency    = 32
	defaultCachelimit     = 4 * 1024 * MiB
)

// Hookup to the testing framework
func Test(t *testing.T) { chk.TestingT(t) }

type cmdIntegrationSuite struct{}

var _ = chk.Suite(&cmdIntegrationSuite{})
var ctx = context.Background()

const (
	containerPrefix      = "container"
	blobPrefix           = "blob"
	blockBlobDefaultData = "AzCopy Random Test Data"

	blobfsPrefix                 = "blobfs"
	defaultBlobFSFileSizeInBytes = 1000
)

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Will truncate the end of the test name, if there is not enough room for it, followed by the time-based suffix,
// with a non-zero maxLen.
func generateName(prefix string, maxLen int) string {
	// The following lines step up the stack find the name of the test method
	// Note: the way to do this changed in go 1.12, refer to release notes for more info
	var pcs [10]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	name := "TestFoo" // default stub "Foo" is used if anything goes wrong with this procedure
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Func.Name(), "Suite") {
			name = frame.Func.Name()
			break
		} else if !more {
			break
		}
	}
	funcNameStart := strings.Index(name, "Test")
	name = name[funcNameStart+len("Test"):] // Just get the name of the test and not any of the garbage at the beginning
	name = strings.ToLower(name)            // Ensure it is a valid resource name
	textualPortion := fmt.Sprintf("%s%s", prefix, strings.ToLower(name))
	currentTime := time.Now()
	numericSuffix := fmt.Sprintf("%02d%02d%d", currentTime.Minute(), currentTime.Second(), currentTime.Nanosecond())
	if maxLen > 0 {
		maxTextLen := maxLen - len(numericSuffix)
		if maxTextLen < 1 {
			panic("max len too short")
		}
		if len(textualPortion) > maxTextLen {
			textualPortion = textualPortion[:maxTextLen]
		}
	}
	name = textualPortion + numericSuffix
	return name
}

func generateContainerName() string {
	return generateName(containerPrefix, 63)
}

func generateBlobName() string {
	return generateName(blobPrefix, 0)
}

func getContainerURL(c *chk.C, bsu *service.Client) (ctr *container.Client, name string) {
	name = generateContainerName()
	startTime := time.Now().Add(-2 * time.Minute)

	ctrURL, err := bsu.NewContainerClient(name).GetSASURL(sas.ContainerPermissions{Read: true, Write: true, Create: true, Execute: true}, 
		time.Now().Add(time.Hour),
		&container.GetSASURLOptions{StartTime: &startTime})
	if err != nil {
		panic(err)
	}

	ctr, err = container.NewClientWithNoCredential(ctrURL, nil)
	if err != nil {
		panic(err)
	}
	return ctr, name
}

func getBlockBlobURL(c *chk.C, container *container.Client, prefix string) (blob *blockblob.Client, name string) {
	name = prefix + generateBlobName()
	blob = container.NewBlockBlobClient(name)

	return blob, name
}

func getReaderToRandomBytes(n int) *bytes.Reader {
	r, _ := getRandomDataAndReader(n)
	return r
}

func getRandomDataAndReader(n int) (*bytes.Reader, []byte) {
	data := make([]byte, n, n)
	rand.Read(data)
	return bytes.NewReader(data), data
}

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

func getBSU() *service.Client {
	accountName, accountKey := getAccountAndKey()

	cred, err := blob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	u := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	if err != nil {
		panic(err)
	}

	s, err := service.NewClientWithSharedKeyCredential(u, cred, nil)
	if err != nil {
		panic(err)
	}

	return s
}

func createNewContainer(c *chk.C, bsu *service.Client) (ctr *container.Client, name string) {
	ctr, name = getContainerURL(c, bsu)

	_, err := bsu.CreateContainer(context.TODO(), name, nil)
	c.Assert(err, chk.IsNil)
	return ctr, name
}

func createNewBlockBlob(c *chk.C, container *container.Client, prefix string) (blob *blockblob.Client, name string) {
	blob, name = getBlockBlobURL(c, container, prefix)

	_, err := blob.UploadBuffer(ctx, []byte(blockBlobDefaultData), nil)
	c.Assert(err, chk.IsNil)

	return
}

func createNewDirectoryStub(c *chk.C, container *container.Client, dirPath string) {
	dir := container.NewBlockBlobClient(dirPath)
	trueStr := "true"
	metadata := make(map[string]*string)
	metadata["hdi_isfolder"] = &trueStr

	_, err := dir.UploadStream(ctx, bytes.NewReader(nil), &blockblob.UploadStreamOptions{Metadata: metadata})

	c.Assert(err, chk.IsNil)
	return
}

func deleteContainer(c *chk.C, container azblob.ContainerURL) {
	resp, err := container.Delete(ctx, azblob.ContainerAccessConditions{})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, 202)
}

func validateStorageError(c *chk.C, err error, code azblob.ServiceCodeType) {
	serr, _ := err.(azblob.StorageError)
	c.Assert(serr.ServiceCode(), chk.Equals, code)
}

func getRelativeTimeGMT(amount time.Duration) time.Time {
	currentTime := time.Now().In(time.FixedZone("GMT", 0))
	currentTime = currentTime.Add(amount * time.Second)
	return currentTime
}

func generateCurrentTimeWithModerateResolution() time.Time {
	highResolutionTime := time.Now().UTC()
	return time.Date(highResolutionTime.Year(), highResolutionTime.Month(), highResolutionTime.Day(), highResolutionTime.Hour(), highResolutionTime.Minute(),
		highResolutionTime.Second(), 0, highResolutionTime.Location())
}

func cleanBlobAccount(c *chk.C, serviceURL azblob.ServiceURL) {
	marker := azblob.Marker{}
	for marker.NotDone() {
		resp, err := serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})
		c.Assert(err, chk.IsNil)

		for _, v := range resp.ContainerItems {
			_, err = serviceURL.NewContainerURL(v.Name).Delete(ctx, azblob.ContainerAccessConditions{})
			c.Assert(err, chk.IsNil)
		}

		marker = resp.NextMarker
	}
}

type stringContainsChecker struct {
	*chk.CheckerInfo
}

var StringContains = &stringContainsChecker{
	&chk.CheckerInfo{Name: "StringContains", Params: []string{"obtained", "expected to find"}},
}

func (checker *stringContainsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) < 2 {
		return false, "StringContains requires two parameters"
	} // Ignore extra parameters

	// Assert that params[0] and params[1] are strings
	aStr, aOK := params[0].(string)
	bStr, bOK := params[1].(string)
	if !aOK || !bOK {
		return false, "All parameters must be strings"
	}

	if strings.Contains(aStr, bStr) {
		return true, ""
	}

	return false, fmt.Sprintf("Failed to find substring in source string:\n\n"+
		"SOURCE: %s\n"+
		"EXPECTED: %s\n", aStr, bStr)
}

// create a test file
func generateFile(fileName string, fileSize int) []byte {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// write to file and return the data
	ioutil.WriteFile(fileName, bigBuff, 0666)
	return bigBuff
}
