package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/wastore/go-lustre/hsm"
	"github.com/wastore/go-lustre/llapi"
	"github.com/wastore/lemur/cmd/lhsmd/agent/fileid"
)

// Construct a FileInfo compatible struct.
type myFileInfo struct {
	name string         // base name of the file
	stat syscall.Stat_t // underlying data source (can return nil)
}

func (fi *myFileInfo) Name() string {
	return fi.name
}

func (fi *myFileInfo) Size() int64 {
	return fi.stat.Size
}

func (fi *myFileInfo) Mode() os.FileMode {
	return os.FileMode(fi.stat.Mode)
}

func (fi *myFileInfo) ModTime() time.Time {
	return time.Unix(fi.stat.Mtim.Sec, fi.stat.Mtim.Nsec)
}

func (fi *myFileInfo) IsDir() bool {
	return fi.Mode().IsDir()
}
func (fi *myFileInfo) Sys() interface{} {
	return &fi.stat
}


func main() {

	accountName, accountKey := os.Getenv("STORAGE_ACCOUNT"), os.Getenv("STORAGE_KEY")
	if len(accountName) == 0 {
		log.Fatal("The STORAGE_ACCOUNT environment variable is not set")
	}
	if len(accountKey) == 0 {
		log.Fatal("The STORAGE_KEY environment variable is not set")
	}

	containerName := os.Args[1]

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}

	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s?include=metadata", accountName, containerName)

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	container, err := container.NewClientWithSharedKeyCredential(url, credential, &container.ClientOptions{})
	if err != nil {
		log.Fatal("Could not create container client")
	}
	ctx := context.Background()

	dirs := make(map[string]bool)

    const MAX = 256
    sem := make(chan int, MAX)
    var wg sync.WaitGroup

    importHSM := func(containerName string, name string, uid uint32, gid uint32, perm uint32, sz int64, sec int64, nsec int64) {
	defer wg.Done()

	fi := &myFileInfo{}
	fi.name = name

	stat := &fi.stat
	stat.Uid = uid
	stat.Gid = gid
	stat.Mode = perm
	stat.Size = sz
	stat.Atim.Sec = sec
	stat.Atim.Nsec = nsec
	stat.Mtim.Sec = sec
	stat.Mtim.Nsec = nsec

	layout := llapi.DefaultDataLayout()
	//layout.StripeCount = 1
	//layout.StripeSize = 1 << 20
	//layout.PoolName = ""

	archive := uint(1)

	_, err = hsm.Import(name, archive, fi, layout)
	if err != nil {
	    log.Fatal(err)
	}

	uuid := fmt.Sprintf("az://%s/%s", containerName, name)
	fileid.UUID.Set(name, []byte(uuid))

	<-sem
    }
    
    pager := container.NewListBlobsFlatPager(nil)
    for pager.More() {
	resp, err := pager.NextPage(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	for _, blobInfo := range resp.Segment.BlobItems {
		dir := filepath.Dir(*blobInfo.Name)
		if dir != "." {
			if _, ok := dirs[dir]; !ok {
				dirs[dir] = true
				os.MkdirAll(dir, 0777)
				//fmt.Printf("mkdir -p %s\n", dir)
			}
		}
		uid := uint32(1000)
		if val, ok := blobInfo.Metadata["uid"]; ok {
			val2, err := strconv.ParseUint(*val, 10, 32)
			if err == nil {
				uid = uint32(val2)
			}
		}
		gid := uint32(1000)
		if val, ok := blobInfo.Metadata["gid"]; ok {
			val2, err := strconv.ParseUint(*val, 10, 32)
			if err == nil {
				gid = uint32(val2)
			}
		}
		perm := uint32(420)
		if val, ok := blobInfo.Metadata["perm"]; ok {
			val2, err := strconv.ParseUint(*val, 8, 32)
			if err == nil {
				perm = uint32(val2)
			}
		}
		modtime := time.Now()
		if val, ok := blobInfo.Metadata["modtime"]; ok {
			val2, err := time.Parse("2006-01-02 15:04:05 -0700", *val)
			if err == nil {
				modtime = val2
			}
		}

		sem <- 1
		wg.Add(1)
		importHSM(containerName,
			  *blobInfo.Name,
			  uid, gid, perm,
			  *blobInfo.Properties.ContentLength,
			  int64(modtime.Unix()),
			  int64(modtime.Nanosecond()))
	}
    }

    wg.Wait()
}
