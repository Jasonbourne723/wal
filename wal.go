package wal

import (
	"errors"
	"log"
	"os"
	"strings"
)

const (
	defaultDirPath = "./"
	defaultExt     = ".seg"
)

var (
	ErrInVaildExt = errors.New("ext invaild")
)

type Option func(*Wal)

type Wal struct {
	currentSegment *Segment
	dirPath        string
	ext            string
	segments       map[uint8]*Segment
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
	// var segment = NewSegment(SegmentFileName(options.dirPath, options.ext, options.id))
	// w.currentSegment = segment
	return nil
}

func NewWal(options ...Option) *Wal {
	wal := &Wal{
		dirPath:  defaultDirPath,
		ext:      defaultExt,
		segments: make(map[uint8]*Segment),
	}

	for _, option := range options {
		option(wal)
	}
	return wal
}

func WithDirPath(dirPath string) Option {
	return func(w *Wal) {
		w.dirPath = dirPath
	}
}

func WithExt(ext string) Option {
	return func(w *Wal) {
		w.ext = ext
	}
}

func (w *Wal) LoadSegment() error {
	entrys, err := os.ReadDir(w.dirPath)
	if err != nil {
		return err
	}

	var currentSegmentId uint8 = 0
	for _, entry := range entrys {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), w.ext) {
			seg, err := OpenSegment(w.dirPath, entry.Name())
			if err != nil {
				log.Fatal(entry.Name(), "open failed")
			}
			w.segments[seg.id] = seg
			if seg.id > currentSegmentId {
				currentSegmentId = seg.id
			}
		}
	}

	if currentSegmentId == 0 {
		currentSegmentId++
		if w.currentSegment, err = NewSegment(w.dirPath, currentSegmentId, w.ext); err != nil {
			return err
		} else {
			w.segments[currentSegmentId] = w.currentSegment
		}
	} else {
		w.currentSegment = w.segments[currentSegmentId]
	}
	return nil
}

func (w *Wal) Write(data []byte) (ChunkPosition, error) {
	return w.currentSegment.Write(data)
}

func (w *Wal) Read(position ChunkPosition) []byte {

	seg, ok := w.segments[position.SegmentId]
	if !ok {
		return nil
	}

	if data, err := seg.Read(position); err != nil {
		panic(err)
	} else {
		return data
	}
}
