package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

const (
	blockSize    = 32 * KB
	fileModePerm = 0644
)

type Segment struct {
	id                 uint8
	fd                 *os.File
	currentBlockNumber int
	currentBloockSize  int
	closed             bool
}

func NewSegment(dirPath string, id uint8, ext string) (*Segment, error) {

	filePath := SegmentFileName(dirPath, id, ext)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, fileModePerm)
	if err != nil {
		panic(err)
	}
	return &Segment{
		id:                 id,
		fd:                 file,
		currentBlockNumber: 0,
		currentBloockSize:  0,
		closed:             false,
	}, nil
}

func OpenSegment(dirPath string, fileName string) (*Segment, error) {

	filePath := path.Join(dirPath, fileName)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, fileModePerm)
	if err != nil {
		panic(err)
	}

	fileNameWithoutSuffix := strings.Split(fileName, ".")[0]

	id, err := strconv.Atoi(fileNameWithoutSuffix)
	if err != nil {
		return nil, err
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &Segment{
		id:                 uint8(id),
		fd:                 file,
		currentBlockNumber: int(offset)/blockSize,
		currentBloockSize:  int(offset),
		closed:             false,
	}, nil
}

func SegmentFileName(dirPath string, id uint8, ext string) string {
	return path.Join(dirPath, fmt.Sprintf("%03d"+ext, id))
}

func (s *Segment) Write(data []byte) (chunkPosition ChunkPosition, err error) {

	chunkBytes := CodeChunk(data)

	if blockSize-s.currentBlockNumber < len(chunkBytes) {
		s.currentBlockNumber += 1
		s.currentBloockSize = 0
	}

	l, err := s.WriteToFile(chunkBytes)
	if err != nil {
		return chunkPosition, err
	}
	chunkPosition = ChunkPosition{
		SegmentId:   s.id,
		BlockNumber: s.currentBlockNumber,
		ChunkOffset: s.currentBloockSize,
	}
	s.currentBloockSize += l
	return
}

func (s *Segment) WriteToFile(data []byte) (int, error) {
	l, err := s.fd.Write(data)
	if err != nil {
		return l, err
	}
	err = s.fd.Sync()
	if err != nil {
		return l, err
	}
	return l, nil
}

func (s *Segment) Read(chunkPosition ChunkPosition) ([]byte, error) {

	offset := chunkPosition.BlockNumber*blockSize + chunkPosition.ChunkOffset

	headerBytes := make([]byte, chunkHeaderSize)
	s.fd.ReadAt(headerBytes, int64(offset))
	header := DecodeChunkHeader(headerBytes)

	data := make([]byte, header.len)
	s.fd.ReadAt(data, int64(offset+chunkHeaderSize))

	if Check(header, data) {
		return data, nil
	} else {
		return nil, errors.New("crc check failed")
	}
}

func (s *Segment) Close() {

	if !s.closed {
		s.fd.Close()
		s.closed = !s.closed
	}
}

type ChunkPosition struct {
	SegmentId   uint8
	BlockNumber int
	ChunkOffset int
}
