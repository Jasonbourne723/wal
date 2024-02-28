package wal

import (
	"fmt"
	"testing"
)

func TestWal(t *testing.T) {

	wal := &Wal{}

	if err := wal.Open(Options{
		dirPath: "D:\\",
		ext:     ".seg",
		id:      "001",
	}); err != nil {
		fmt.Printf("err: %v\n", err)
	}

	str1 := `
	go          break       switch      case    select
	package     import      func        if      else
	const       type        interface   struct  map
	range       chan        defer       default   for    
	return      goto        continue    fallthrough  var `

	str2 := "hello world nihao"

	position1 := wal.Write([]byte(str1))
	position2 := wal.Write([]byte(str2))

	fmt.Printf("wal.Read(position2): %v\n", wal.Read(position2))

	fmt.Printf("wal.Read(position1): %v\n", wal.Read(position1))

}
