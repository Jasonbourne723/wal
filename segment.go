package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
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
	rwMutex            *sync.RWMutex
}

// 创建新的Segment
func NewSegment(dirPath string, id uint8, ext string) (*Segment, error) {

	filePath := BuildSegmentName(dirPath, id, ext)
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
		rwMutex:            new(sync.RWMutex),
	}, nil
}

// 打开Segment
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
		currentBlockNumber: int(offset) / blockSize,
		currentBloockSize:  int(offset),
		closed:             false,
		rwMutex:            new(sync.RWMutex),
	}, nil
}

// 拼接Segment文件名称
func BuildSegmentName(dirPath string, id uint8, ext string) string {
	return path.Join(dirPath, fmt.Sprintf("%03d"+ext, id))
}

// 字节数组写入Segment
func (s *Segment) Write(data []byte) (chunkPosition ChunkPosition, err error) {

	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
	}()
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

// 字节数组写入文件
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

// 读文件
func (s *Segment) Read(chunkPosition ChunkPosition) ([]byte, error) {

	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()

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

func (s *Segment) ReadAll() [][]byte {

	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
	}()

	result := make([][]byte, 0)
	blockNumber := 0

	for {
		datas := s.ReadBlock(blockNumber)
		if len(datas) == 0 {
			break
		}
		blockNumber++
		result = append(result, datas...)
	}
	return result
}

func (s *Segment) ReadBlock(blockNumber int) [][]byte {
	blockData := make([]byte, blockSize)
	s.fd.ReadAt(blockData, int64(blockNumber*blockSize))
	offset := 0
	result := make([][]byte, 0)
	for {

		if offset+chunkHeaderSize >= blockSize {
			break
		}

		headerBytes := blockData[offset : offset+chunkHeaderSize]
		header := DecodeChunkHeader(headerBytes)
		if header.len > 0 {
			data := blockData[offset+chunkHeaderSize : offset+chunkHeaderSize+int(header.len)]
			if Check(header, data) {
				result = append(result, data)
			} else {
				break
			}
		} else {
			break
		}
		offset += chunkHeaderSize + int(header.len)
	}
	return result
}

// 关闭文件
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
