package wal

import (
	"fmt"
	"testing"
)

func TestWal(t *testing.T) {

	wal := NewWal()
	if err := wal.LoadSegment(); err != nil {
		t.Error(err)
	}
	position1, err := wal.Write([]byte("hello world"))
	if err != nil {
		t.Error(err)
	}
	position2, err := wal.Write([]byte("hello xudong"))
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("wal.Read(position1): %v\n", string(wal.Read(position1)))
	fmt.Printf("wal.Read(position2): %v\n", string(wal.Read(position2)))
}
