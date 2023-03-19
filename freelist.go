package main

import (
	"encoding/binary"
)


const (
	initialPage = 0
)

type freeList struct {
	maxPage pageNumber
	releasedPages []pageNumber

}


func newFreeList() *freeList {
	return &freeList{
		maxPage: initialPage,
		releasedPages: []pageNumber{},
	}
}

func (fr *freeList) getNextPage() pageNumber {
	if len(fr.releasedPages) != 0 {
		pageID := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pageID
	}
	fr.maxPage += 1
	return fr.maxPage
}

func (f *freeList) releasePage(number pageNumber){
	f.releasedPages = append(f.releasedPages, number) 
}



func (fr *freeList) serialize(buf []byte) []byte {
	pos := 0
	binary.LittleEndian.PutUint16(buf[pos:], uint16(fr.maxPage))
	pos += 2
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(fr.releasedPages)))
	pos += 2

	for _, page := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumberSize

	}
	return buf
}

func (fr *freeList) deserialize(buf []byte) {
	pos := 0
	fr.maxPage = pageNumber(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	releasedPagesCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPagesCount; i++ {
		fr.releasedPages = append(fr.releasedPages, pageNumber(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumberSize
	}
}