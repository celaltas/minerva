package main

import "encoding/binary"

const (
	metaPageNumber = 0
	pageNumberSize = 8
)

type meta struct {
	freeListPage pageNumber
}

func newMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buffer []byte) {
	position := 0
	binary.LittleEndian.PutUint64(buffer[position:], uint64(m.freeListPage))
	position += pageNumberSize

}
func (m *meta) deserialize(buffer []byte) {
	position := 0
	m.freeListPage = pageNumber(binary.LittleEndian.Uint64(buffer[position:]))
	position += pageNumberSize
}
