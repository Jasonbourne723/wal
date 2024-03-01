# wal

## 特性

- 基于磁盘，支持大数据量
- 仅追加写入，高性能
- 快速读取，一个磁盘查找以检索任何值
- 支持块缓存，提高读取性能
- 支持并发写入和读取，所有函数都是线程安全的

## 设计总览

![](wal.png)

## Demo

``` go
func TestWal(t *testing.T) {

	wal := NewWal()
	if err := wal.LoadSegment(); err != nil {
		t.Error(err)
	}
    
	position1, err := wal.Write([]byte("hello world"))
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("wal.Read(position1): %v\n", string(wal.Read(position1)))
}
```