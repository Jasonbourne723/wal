package wal

import "encoding/binary"

type QuickKV struct {
	data map[string][]byte
	wal  *Wal
}

func NewQuickKV() *QuickKV {

	wal := NewWal()
	wal.LoadSegment()
	kv := &QuickKV{
		data: make(map[string][]byte, 10),
		wal:  wal,
	}
	datas := wal.ReadAll()
	for _, v := range datas {
		key, val := DeCodeToKeyValue(v)
		kv.data[key] = val
	}

	return kv
}

func (q *QuickKV) Set(key string, value []byte) error {

	_, err := q.wal.Write(EnCodeKeyValue(key, value))
	if err != nil {
		return err
	}
	q.data[key] = value
	return nil
}

func (q *QuickKV) Get(key string) ([]byte, bool) {

	if val, ok := q.data[key]; ok {

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
