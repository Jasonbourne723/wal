package wal

import "os"

type ChunkHeader uint16

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

const (
	chunkHeaderSize = 2
	blockSize       = 32 * KB
	fileModePerm    = 0644
)

type Segment struct {
	fd                 *os.File
	currentBlockNumber int
	currentBloockSize  int
	closed             bool
}

func NewSegment(filePath string) *Segment {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, fileModePerm)
	if err != nil {
		panic(err)
	}
	return &Segment{
		fd:                 file,
		currentBlockNumber: 0,
		currentBloockSize:  0,
		closed:             true,
	}
}

func (s *Segment) Close() {

	if !s.closed {
		s.fd.Close()
		s.closed = !s.closed
	}
}

type ChunkPosition struct {
	BlockNumber int
	ChunkOffset int
	ChunkSize   int
}
