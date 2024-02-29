package wal

import "encoding/binary"

type QuickKV struct {
	data map[string]ChunkPosition
	wal  *Wal
}

func NewQuickKV() *QuickKV {

	wal := NewWal()
	wal.LoadSegment()
	return &QuickKV{
		data: make(map[string]ChunkPosition, 10),
		wal:  wal,
	}
}

func (q *QuickKV) Set(key string, value []byte) error {

	position, err := q.wal.Write(EnCodeKeyValue(key, value))
	if err != nil {
		return err
	}
	q.data[key] = position
	return nil
}

func (q *QuickKV) Get(key string) ([]byte, bool) {

	if position, ok := q.data[key]; ok {
		bytes := q.wal.Read(position)
		_, val := DeCodeToKeyValue(bytes)
		return val, true
	}
	return nil, false

}

func EnCodeKeyValue(key string, value []byte) []byte {

	bytes := make([]byte, 0)

	bytes = binary.BigEndian.AppendUint16(bytes, uint16(len(key)))
	bytes = append(bytes, []byte(key)...)
	bytes = append(bytes, value...)

	return bytes
}

func DeCodeToKeyValue(bytes []byte) (string, []byte) {

	keyLen := binary.BigEndian.Uint16(bytes[:2])
	key := string(bytes[2 : 2+keyLen])
	value := bytes[2+keyLen:]
	return key, value
}
