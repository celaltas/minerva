package main

import (
	"errors"
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
	*freeList
	*meta
}

func newDal(path string) (*dal, error) {
	dal:=&dal{
		meta: newMeta(),
		pageSize: os.Getpagesize(),
	}
	if _,err:=os.Stat(path);err==nil{
		dal.file,err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil{
			dal.close()
			return nil, err
		}
		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freeList, err := dal.readFreelist()
		if err != nil {
			return nil, err
		}
		dal.freeList = freeList

	}else if errors.Is(err, os.ErrNotExist){
		dal.file,err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil{
			dal.close()
			return nil, err
		}
		dal.freeList = newFreeList()
		dal.freeListPage = dal.freeList.getNextPage()
		_,err:=dal.writeFreeList()
		if err != nil {
			return nil, err
		}
		_, err = dal.writeMeta(dal.meta)

	}else{
		return nil, err
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


func (d *dal) writeMeta(meta *meta) (*page, error) {
	page:= d.allocateEmptyPage()
	page.number = metaPageNumber
	meta.serialize(page.data)
	if err:=d.writePage(page); err != nil{
		return nil, err
	}
	return page, nil
}

func (d *dal) readMeta() (*meta, error){

	page,err:= d.readPage(metaPageNumber)
	if err != nil {
		return nil, err
	}
	meta:=newMeta()
	meta.deserialize(page.data)
	return meta, nil
}


func (d *dal) writeFreeList() (*page, error){
	page:= d.allocateEmptyPage()
	page.number = d.freeListPage
	d.freeList.serialize(page.data)
	err:=d.writePage(page)
	if err != nil {
		return nil, err
	}
	d.freeListPage = page.number
	return page, nil
}


func (d *dal) readFreelist() (*freeList, error) {
	p, err := d.readPage(d.freeListPage)
	if err != nil {
		return nil, err
	}

	freeList := newFreeList()
	freeList.deserialize(p.data)
	return freeList, nil
}

