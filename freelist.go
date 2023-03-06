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

func (f *freeList) getNextPage() pageNumber {
	if len(f.releasedPages) != 0 {
		number := f.releasedPages[len(f.releasedPages)-1]
		f.releasedPages = f.releasedPages[:len(f.releasedPages)-1]
		return number
	}
	f.maxPage++
	return f.maxPage

}

func (f *freeList) releasePage(number pageNumber){
	f.releasedPages = append(f.releasedPages, number) 
}



func (f *freeList) serialize(buffer []byte) []byte {
	position := 0
	binary.LittleEndian.PutUint16(buffer[position:], uint16(f.maxPage))
	position +=2
	binary.LittleEndian.PutUint16(buffer[position:], uint16(len(f.releasedPages)))
	position +=2
	for _,page:= range f.releasedPages{
		binary.LittleEndian.PutUint64(buffer[position:], uint64(page))
		position += pageNumberSize
	}
	return buffer

}

func (f *freeList) deserialize(buffer []byte) {
	position := 0
	f.maxPage=pageNumber(binary.LittleEndian.Uint16(buffer[position:]))
	position +=2
	releasedPageCount := int(binary.LittleEndian.Uint16(buffer[position:]))
	position +=2
	for i:=0; i < releasedPageCount; i++ {
		f.releasedPages = append(f.releasedPages, pageNumber(binary.LittleEndian.Uint64(buffer[position:])))
		position += pageNumberSize
	}
}