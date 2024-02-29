package wal

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	chunkHeaderSize = 6
)

type ChunkHeader struct {
	crc uint32
	len uint16
}

func CodeChunk(data []byte) []byte {

	b := make([]byte, 0)
	b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(data))
	b = binary.BigEndian.AppendUint16(b, uint16(len(data)))

	return append(b, data...)
}

func DecodeChunkHeader(bytes []byte) ChunkHeader {
	return ChunkHeader{
		crc: binary.BigEndian.Uint32(bytes[:4]),
		len: binary.BigEndian.Uint16(bytes[4:]),
	}
}

func Check(header ChunkHeader, data []byte) bool {

	return crc32.ChecksumIEEE(data) == header.crc
}
