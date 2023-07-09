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
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	kvauth "github.com/Azure/azure-sdk-for-go/services/keyvault/auth"
	"github.com/Azure/azure-sdk-for-go/services/keyvault/v7.0/keyvault"
	"golang.org/x/sys/unix"
)

var authFailureError = []bloberror.Code {
	bloberror.AuthenticationFailed,
	bloberror.AuthorizationFailure,
	bloberror.AuthorizationPermissionMismatch,
	bloberror.AuthorizationProtocolMismatch,
	bloberror.AuthorizationResourceTypeMismatch,
	bloberror.AuthorizationServiceMismatch,
	bloberror.AuthorizationSourceIPMismatch,
	bloberror.InsufficientAccountPermissions,
	bloberror.InvalidAuthenticationInfo,
	bloberror.UnauthorizedBlobOverwrite,
}

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

var CopytoolBusy = ErrorEx{code: int32(syscall.EBUSY), msg: "Too many requests on copytool" }

func ShouldRetry(err error) bool {
	return bloberror.HasCode(err, authFailureError...)
}

func ShouldRefreshCreds(err error) bool {
	return bloberror.HasCode(err, authFailureError...)
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
	if secret[0] == '?' {
		secret = secret[1:]
	}

	return secret, nil
}

func IsSASValid(sas string) (ok bool, reason string) {
	if sas == "" {
		return false, "Empty string returned."
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

func UnixError(err error) (ret int32) {
	if err == nil {
		ret = int32(0)
	}

	if errEx, ok := err.(ErrorEx); ok {
		ret = errEx.ErrorCode()
	} else {
		ret = int32(syscall.EINVAL)
	}

    // Return 1 (EPERM 1 Operation not permitted) for all non-standard UNIX ERRNOs
	errno_name := unix.ErrnoName(syscall.Errno(ret))
	msg := fmt.Sprintf("For error %v, returned status %d", err, ret)
	if errno_name == "" {
		val := ret
		ret = 1
		msg = fmt.Sprintf("No standard Unix errno for '%v', status: %d. Will return status %d", err, val, ret)
	}
	Log(pipeline.LogError, msg)
	return ret
}