package main

import "encoding/binary"

const (
	metaPageNumber = 0
	pageNumberSize = 8
	nodeHeaderSize = 3
)

type meta struct {
	root pageNumber
	freeListPage pageNumber
}

func newMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.root))
	pos += pageNumberSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freeListPage))
	pos += pageNumberSize
}
func (m *meta) deserialize(buf []byte) {
	pos := 0

	m.root = pageNumber(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumberSize

	m.freeListPage = pageNumber(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumberSize
}
