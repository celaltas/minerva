package main

import "encoding/binary"

const (
	metaPageNumber = 0
	pageNumberSize = 8
	nodeHeaderSize = 3
	magicNumber uint32 = 0xD00DB00D
	magicNumberSize = 4
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
	binary.LittleEndian.PutUint32(buf[pos:], magicNumber)
	pos += magicNumberSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.root))
	pos += pageNumberSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freeListPage))
	pos += pageNumberSize
}
func (m *meta) deserialize(buf []byte) {
	pos := 0
	magicNumberRes := binary.LittleEndian.Uint32(buf[pos:])
	pos += magicNumberSize

	if magicNumberRes != magicNumber {
		panic("The file is not a libra db file")
	}

	m.root = pageNumber(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumberSize

	m.freeListPage = pageNumber(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumberSize
}
