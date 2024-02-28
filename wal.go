package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
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
	id      string
	ext     string
}

func (w *Wal) Open(options Options) error {

	if !strings.HasPrefix(options.ext, ".") {
		return ErrInVaildExt
	}
	segmentName := path.Join(options.dirPath, options.id+options.ext)

	var segment = &Segment{}

	file, err := os.OpenFile(segmentName, os.O_APPEND|os.O_RDWR, fileModePerm)
	if err != nil {
		return errors.New("file open failed")
	}
	segment.fd = file
	w.seg = segment
	return nil
}

func (w *Wal) Write(data []byte) (result ChunkPosition) {

	l := len(data)
	chunkHeader := ChunkHeader(l)
	l += 2

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
		ChunkSize:   l - 2,
	}
}

func (w *Wal) Read(position ChunkPosition) string {

	offset := position.BlockNumber*blockSize + position.ChunkOffset + 2

	bytes := make([]byte, position.ChunkSize)
	w.seg.fd.ReadAt(bytes, int64(offset))

	return string(bytes)
}
