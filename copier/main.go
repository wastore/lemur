package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	copier "github.com/wastore/lemur/copier/core"
)

const MiB = 1024 * 1024

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

func main() {
	copier := copier.NewCopier(0, 4000*MiB, 4000*MiB, 16)
	s := getBSU()
	container1 := s.NewContainerClient("container1")

	args := os.Args[1:]

	bb := container1.NewBlockBlobClient(args[1])

    var n int64
    var err error
	if args[0] == "d" {
		n, err = copier.DownloadFile(context.TODO(), bb, args[2], nil)
    } else {
        err = copier.UploadFile(context.TODO(), bb, args[2], nil)
    }
	fmt.Printf("%d %v\n", n, err)
}
