package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

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
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)
	ctx := context.Background()

	dirs := make(map[string]bool)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			log.Fatal(err)
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			dir := filepath.Dir(blobInfo.Name)
			if dir != "." {
				if _, ok := dirs[dir]; !ok {
					dirs[dir] = true
					fmt.Printf("mkdir -p %s\n", dir)
				}
			}

			fmt.Println(blobInfo)
			uid := 1000
			if val, ok := blobInfo.Metadata["Uid"]; ok {
				val2, err := strconv.ParseInt(val, 10, 32)
				if err == nil {
					uid = int(val2)
				}
			}
			gid := 1000
			if val, ok := blobInfo.Metadata["Uid"]; ok {
				val2, err := strconv.ParseInt(val, 10, 32)
				if err == nil {
					gid = int(val2)
				}
			}
			perm := 420
			if val, ok := blobInfo.Metadata["Perm"]; ok {
				val2, err := strconv.ParseInt(val, 8, 32)
				if err == nil {
					perm = int(val2)
				}
			}
			modtime := time.Now().String()
			if val, ok := blobInfo.Metadata["ModTime"]; ok {
				modtime = val
			}
			fmt.Printf("sudo lhsm import --uuid \"az://%s/%s\" --mtime \"%s\" --mode %d --uid %d --gid %d -id %d --size %d %s\n",
				containerName, blobInfo.Name, modtime, perm, uid, gid, 1, *blobInfo.Properties.ContentLength, blobInfo.Name)
		}
	}

}
