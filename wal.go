package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strings"
)

var (
	ErrInVaildExt = errors.New("ext invaild")
)

type Wal struct {
	seg *Segment
}

type Options struct {
	dirPath string
	id      int
	ext     string
}

func (w *Wal) Open(options Options) error {

	if !strings.HasPrefix(options.ext, ".") {
		return ErrInVaildExt
	}
	var segment = NewSegment(SegmentFileName(options.dirPath, options.ext, options.id))
	w.seg = segment
	return nil
}

func SegmentFileName(dirPath string, ext string, id int) string {
	return path.Join(dirPath, fmt.Sprintf("%03d"+ext, id))
}

func (w *Wal) Write(data []byte) (result ChunkPosition) {

	l := len(data)
	chunkHeader := ChunkHeader(l)
	l += chunkHeaderSize

	if w.seg.currentBloockSize+l >= blockSize {
		w.seg.currentBlockNumber++
		w.seg.currentBloockSize = 0
	}

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, chunkHeader)
	w.seg.fd.Write(bytesBuffer.Bytes())
	w.seg.fd.Write(data)
	w.seg.currentBloockSize += l

	return ChunkPosition{
		BlockNumber: w.seg.currentBlockNumber,
		ChunkOffset: w.seg.currentBloockSize - l,
		ChunkSize:   l - chunkHeaderSize,
	}
}

func (w *Wal) Read(position ChunkPosition) string {

	lenOffset := position.BlockNumber*blockSize + position.ChunkOffset
	lenBytes := make([]byte, 2)
	w.seg.fd.ReadAt(lenBytes, int64(lenOffset))

	var l = int(binary.BigEndian.Uint16(lenBytes))

	writeBytes := make([]byte, l)
	w.seg.fd.ReadAt(writeBytes, int64(lenOffset+2))
	return string(writeBytes)
}
