package main

import (
	"errors"
	"fmt"
	"os"
)

type pageNumber uint64

type Options struct {
	pageSize       int
	MinFillPercent float32
	MaxFillPercent float32
}

var DefaultOptions = &Options{
	MinFillPercent: 0.5,
	MaxFillPercent: 0.95,
}

type page struct {
	number pageNumber
	data   []byte
}

type dal struct {
	file           *os.File
	pageSize       int
	minFillPercent float32
	maxFillPercent float32
	*freeList
	*meta
}

func newDal(path string, options *Options) (*dal, error) {
	dal := &dal{
		meta:           newMeta(),
		pageSize:       options.pageSize,
		minFillPercent: options.MinFillPercent,
		maxFillPercent: options.MaxFillPercent,
	}

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.readFreelist()
		if err != nil {
			return nil, err
		}
		dal.freeList = freelist
	} else if errors.Is(err, os.ErrNotExist) {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		dal.freeList = newFreeList()
		dal.freeListPage = dal.getNextPage()
		_, err := dal.writeFreeList()
		if err != nil {
			return nil, err
		}

		collectionsNode, err := dal.writeNode(NewNodeForSerialization([]*Item{}, []pageNumber{}))
		if err != nil {
			return nil, err
		}
		dal.root = collectionsNode.pageNum

		_, err = dal.writeMeta(dal.meta) 
	} else {
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
	return &page{
		data: make([]byte, d.pageSize, d.pageSize),
	}
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
	page := d.allocateEmptyPage()
	page.number = metaPageNumber
	meta.serialize(page.data)
	if err := d.writePage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func (d *dal) readMeta() (*meta, error) {

	page, err := d.readPage(metaPageNumber)
	if err != nil {
		return nil, err
	}
	meta := newMeta()
	meta.deserialize(page.data)
	return meta, nil
}

func (d *dal) writeFreeList() (*page, error) {
	page := d.allocateEmptyPage()
	page.number = d.freeListPage
	d.freeList.serialize(page.data)
	err := d.writePage(page)
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


func (d *dal) newNode(items []*Item, childNodes []pageNumber) *Node {
	node := NewNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = d.getNextPage()
	node.dal = d
	return node
}

func (d *dal) getNode(number pageNumber) (*Node, error) {
	p, err := d.readPage(number)
	if err != nil {
		return nil, err
	}
	node := NewNode()
	node.deserialize(p.data)
	node.pageNum = number
	node.dal=d
	return node, nil
}

func (d *dal) writeNode(n *Node) (*Node, error) {
	p := d.allocateEmptyPage()
	if n.pageNum == 0 {
		p.number = d.getNextPage()
		n.pageNum = p.number
	} else {
		p.number = n.pageNum
	}
	p.data = n.serialize(p.data)
	if err := d.writePage(p); err != nil {
		return nil, err
	}
	return n, nil

}

func (d *dal) deleteNode(number pageNumber) {
	d.releasePage(number)
}

func (d *dal) maxThreshold() float32 {
	return d.maxFillPercent * float32(d.pageSize)
}

func (d *dal) isOverPopulated(node *Node) bool {
	return float32(node.nodeSize()) > d.maxThreshold()
}

func (d *dal) minThreshold() float32 {
	return d.minFillPercent * float32(d.pageSize)
}

func (d *dal) isUnderPopulated(node *Node) bool {
	return float32(node.nodeSize()) < d.minThreshold()
}

func (d *dal) getSplitIndex(node *Node) int {
	size := 0
	size += nodeHeaderSize

	for i := range node.items {
		size += node.elementSize(i)
		if float32(size) > d.minThreshold() && i < len(node.items) - 1 {
			return i + 1
		}
	}

	return -1
}
