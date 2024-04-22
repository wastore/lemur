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
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/intel-hpdd/logging/debug"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"golang.org/x/sys/unix"
)
	//"github.com/Azure/azure-sdk-for-go/services/keyvault/v7.0/keyvault"
	//"github.com/Azure/go-autorest/autorest/azure/auth"

const KV_URL_FIELD_COUNT int = 4

var authFailureError = []bloberror.Code{
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

var CopytoolBusy = ErrorEx{code: int32(syscall.EBUSY), msg: "Too many requests on copytool"}

func ShouldRetry(err error) bool {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}
	return (respErr.StatusCode == http.StatusForbidden)
}

func ShouldRefreshCreds(err error) bool {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}
	return (respErr.StatusCode == http.StatusForbidden)
}

// Extract the base uri from the key vault URL
//
// Key Vault Object Identifiers have the format
// https://{vault-name}.vault.azure.net/{object-type}/{object-name}/{object-version}
//
// Where the {vault-name} has the following restrictions:
//   Vault name must be a 3-24 character string,
//   containing only 0-9, a-z, A-Z, and not consecutive -.
//
// Therefore extraction of the baseUri from the key vault
// URL only requires removal of the vault name from the host.
//
// https://learn.microsoft.com/en-us/azure/key-vault/general/about-keys-secrets-certificates
func GetKvBaseUri(kvURL string) (string, error) {
	debug.Printf("GetKvBaseUri called with %v\n", kvURL)

	parsedKvUrl, err := url.Parse(kvURL)
	if err != nil {
		return "", err
	}

	urlScheme := parsedKvUrl.Scheme
	if urlScheme != "https" {
		return "", fmt.Errorf("Invalid URL scheme. Got '%s'. Expected 'https'. URL: %s", urlScheme, kvURL)
	}

	urlHost := parsedKvUrl.Host
	hostFragments := strings.Split(urlHost, ".")
	if len(hostFragments) != KV_URL_FIELD_COUNT {
		return "", fmt.Errorf("Invalid URL host. Got '%s'. Expected format {vault-name}.vault.{mid-domain}.{top-domain}", urlHost)
	}

	baseUri := url.URL{
		Scheme: urlScheme,
		Host:   strings.Join(hostFragments[1:], "."),
	}

	return baseUri.String(), nil
}


// GetKVSecret returns string secret by name 'kvSecretName' in keyvault 'kvName'
// Uses MSI auth to login
func GetKVSecret(kvURL, kvSecretName string) (secret string, err error) {
	uami_id := os.Getenv("HSM_COMMON_KEY_VAULT_CLIENT_ID")
	if uami_id == "" {
		return "", fmt.Errorf("Failed to find 'HSM_COMMON_KEY_VAULT_CLIENT_ID' environment variable\n")
	}

	debug.Printf("Creating NewManagedIdentityCredential\n")
	clientId := azidentity.ClientID(uami_id)
	opts := azidentity.ManagedIdentityCredentialOptions{ID: clientId}
	cred, err := azidentity.NewManagedIdentityCredential(&opts)
	if err != nil {
		return "", fmt.Errorf("azidentity.NewDefaultAzureCredential failed: %w", err)
	}

	client, err := azsecrets.NewClient(kvURL, cred, nil)
	if err != nil {
		return "", fmt.Errorf("azsecrets.NewClient failed to create new client for key-vault '%s': %w", kvURL, err)
	}

	// Empty version string gets the latest version of a secret
	//ctx, _:= context.WithTimeoutCause(context.Background(), 3*time.Minute, context.DeadlineExceeded)
	debug.Printf("Attempting to get Key Vault Secret form kvURL: %s\n", kvURL)
	version := ""
	secretResp, err := client.GetSecret(context.TODO(), kvSecretName, version, nil)
	if err != nil {
		return "", fmt.Errorf("Failed to get key vault secret: %w", err)
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
		return false, "Missing signature"
	}
	if endTime := q.Get("se"); endTime == "" {
		return false, "Missing endTime"
	} else {
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			return false, "Invalid expiry time on SAS." + err.Error()
		}
		if t.Before(time.Now()) {
			return false, "Expired SAS returned"
		}
	}

	if v := os.Getenv("COPYTOOL_IGNORE_SAS_PERMISSIONS"); v != "" {
		ignore, err := strconv.ParseBool(v)
		if err == nil && ignore {
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
	} else if (err == context.Canceled) {
    ret = int32(syscall.ECANCELED)
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
