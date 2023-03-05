package main

import (
	"fmt"
	"os"
)

type pageNumber uint64

type page struct {
	number pageNumber
	data   []byte
}

type dal struct {
	file     *os.File
	pageSize int
	freeList *freeList
}

func newDal(path string, pageSize int) (*dal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dal := &dal{
		file:     file,
		pageSize: pageSize,
		freeList: newFreeList(),
	}
	return dal, nil
}

func (d *dal) close() error {
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil

	}
	return nil
}

func (d *dal) allocateEmptyPage() *page {
	return &page{data: make([]byte, d.pageSize)}
}

func (d *dal) readPage(number pageNumber) (*page, error) {
	page := d.allocateEmptyPage()
	offset := d.pageSize * int(number)
	_, err := d.file.ReadAt(page.data, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("error when reading page: %s", err)
	}
	return page, nil

}

func (d *dal) writePage(page *page) error {
	offset := int64(page.number) * int64(d.pageSize)
	_, err := d.file.WriteAt(page.data, offset)
	return err

}
