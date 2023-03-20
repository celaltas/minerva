package main

import "errors"

var writeInsideReadTxErr = errors.New("can't perform a write operation inside a read transaction")

type Tx struct {
	dirtyNodes     map[pageNumber]*Node
	pagesToDelete  []pageNumber
	allocatedPages []pageNumber
	write          bool
	db             *DB
}

func NewTx(db *DB, write bool) *Tx {

	return &Tx{
		map[pageNumber]*Node{},
		make([]pageNumber, 0),
		make([]pageNumber, 0),
		write,
		db,
	}
}



func (tx *Tx) Rollback() {

	if !tx.write {
		tx.db.rwlock.RUnlock()
		return
	}else{
		tx.dirtyNodes=nil
		tx.pagesToDelete=nil
		for _, page:= range tx.allocatedPages{
			tx.db.freeList.releasePage(page)
		}
		tx.allocatedPages = nil
		tx.db.rwlock.Unlock()
	}

}


func (tx *Tx) Commit() error {
	if !tx.write {
		tx.db.rwlock.RUnlock()
		return nil
	}

	for _, node := range tx.dirtyNodes {
		_, err := tx.db.writeNode(node)
		if err != nil {
			return err
		}
	}

	for _, pageNum := range tx.pagesToDelete {
		tx.db.deleteNode(pageNum)
	}
	_, err := tx.db.writeFreeList()
	if err != nil {
		return err
	}

	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	tx.allocatedPages = nil
	tx.db.rwlock.Unlock()
	return nil
}

func (tx *Tx) newNode(items []*Item, childNodes []pageNumber) *Node {
	node := NewNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = tx.db.getNextPage()
	node.tx = tx
	node.tx.allocatedPages = append(node.tx.allocatedPages, node.pageNum)
	return node
}

func (tx *Tx) getNode(pageNum pageNumber) (*Node, error) {
	if node, ok := tx.dirtyNodes[pageNum]; ok {
		return node, nil
	}

	node, err := tx.db.getNode(pageNum)
	if err != nil {
		return nil, err
	}
	node.tx = tx
	return node, nil
}

func (tx *Tx) writeNode(node *Node) *Node {
	tx.dirtyNodes[node.pageNum] = node
	node.tx = tx
	return node
}

func (tx *Tx) deleteNode(node *Node) {
	tx.pagesToDelete = append(tx.pagesToDelete, node.pageNum)
}