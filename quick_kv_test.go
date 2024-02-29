package wal

import (
	"fmt"
	"testing"
)

func TestQuickKV(t *testing.T) {

	kv := NewQuickKV()

	kv.Set("name0", []byte("lilei"))
	kv.Set("name1", []byte("jason"))
	kv.Set("name2", []byte("keyu"))
	kv.Set("name3", []byte("wang"))
	kv.Set("name4", []byte("sun"))

	if val, ok := kv.Get("name2"); ok {
		fmt.Printf("name2: %v\n", string(val))
	}

	if val, ok := kv.Get("name3"); ok {
		fmt.Printf("name5: %v\n", string(val))
	}

	if val, ok := kv.Get("name5"); ok {
		fmt.Printf("name5: %v\n", string(val))
	}

}
