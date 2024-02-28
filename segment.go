package wal

import "os"

type SegmentID uint32
type ChunkHeader uint16

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

const (
	chunkHeaderSize uint32 = 2

	blockSize = 32 * KB

	fileModePerm = 0644
)

type Segment struct {
	//id                 SegmentID
	fd                 *os.File
	currentBlockNumber int
	currentBloockSize  int
	closed             bool
}

type ChunkPosition struct {
	//SegmentId SegmentID
	// BlockNumber The block number of the chunk in the segment file.
	BlockNumber int
	// ChunkOffset The start offset of the chunk in the segment file.
	ChunkOffset int
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	ChunkSize int
}
