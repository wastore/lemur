package copier

import (
    "errors"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)
const (
    MiB = 1024 * 1024
    defaultBlockBlobBlockSize = 8 * MiB
    maxStageBlockBytes = blockblob.MaxStageBlockBytes
    maxNumberOfBlocksPerBlob = blockblob.MaxBlocks
    maxBlobSize = blockblob.MaxBlocks * blockblob.MaxStageBlockBytes
)

var ErrFileTooLarge = errors.New("file too large")

/*
 * Validate source file size and provided block size against block blob limits.
 * If the block size isn't provided, compute a default in powers of two up to
 * the maximum size allowable for StageBlock.  Caller is responsible for
 * determining if the returned block size should result in a call to a single
 * Upload or multiple calls to StageBlock+CommitBlock.
 */
func getBlockSize(blockSize int64, sourceSize int64) (int64, error) {

    // File is simply too huge.
    if (sourceSize > maxBlobSize) {
        return int64(0), ErrFileTooLarge
    }

    // Explicitly given block size works fine
    if (blockSize != 0 && sourceSize <= int64(maxNumberOfBlocksPerBlob * blockSize)) {
        return int64(blockSize), nil
    }

    // At this point either no block size was specified or the given block size
    // is sufficiently small relative to file size such that it would result too
    // many blocks. Start at default and increase in powers of two up to max
    // stage bytes
    for blockSize := defaultBlockBlobBlockSize; blockSize <= maxStageBlockBytes; blockSize *= 2 {
        if sourceSize <= int64(maxNumberOfBlocksPerBlob * blockSize) {
            return int64(blockSize), nil
        }
    }

    // Finally, if we get here, we must require a block size in between the
    // power of two immediately below maxStageBlockBytes and maxStageBlockBytes.
    // For simplicity since we're dealing with huge sizes at this point, just go
    // to max stage bytes
    return int64(maxStageBlockBytes), nil
}
