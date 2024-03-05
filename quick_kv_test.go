package wal

import (
	"fmt"
	"strconv"
	"testing"
)

func TestQuickKV(t *testing.T) {

	kv := NewQuickKV()

	// kv.Set("name0", []byte("lilei"))
	// kv.Set("name1", []byte("jason"))
	// kv.Set("name2", []byte("keyu"))
	// kv.Set("name3", []byte("wang"))
	// kv.Set("name4", []byte("sun"))

	for i := 0; i < 30000; i++ {
		str := strconv.Itoa(i)
		kv.Set("name"+str, []byte(str))
	}

}

func TestWrite(t *testing.T) {

	kv := NewQuickKV()

	// kv.Set("name0", []byte("lilei"))
	// kv.Set("name1", []byte("jason"))
	// kv.Set("name2", []byte("keyu"))
	// kv.Set("name3", []byte("wang"))
	// kv.Set("name4", []byte("sun"))

	for i := 40000; i < 40300; i++ {
		str := strconv.Itoa(i)
		kv.Set("name"+str, []byte(str))
	}

}

func TestRead(t *testing.T) {
	kv := NewQuickKV()

	// if val, ok := kv.Get("name2"); ok {
	// 	fmt.Printf("name2: %v\n", string(val))
	// }
	// if val, ok := kv.Get("name1"); ok {
	// 	fmt.Printf("name1: %v\n", string(val))
	// }

	for i := 0; i < 41000; i++ {
		str := strconv.Itoa(i)
		if val, ok := kv.Get("name" + str); ok {
			fmt.Printf("name%d: %v\n", i, string(val))
		}
	}

	//c := make(chan int, 1)

	//<-time.After(time.Second * 3)

}
