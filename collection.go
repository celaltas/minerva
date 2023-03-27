package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	collectionSize = 16
	counterSize    = 4
)

type Collection struct {
	name    []byte
	root    pageNumber
	tx      *Tx
	counter uint64
}

func NewCollection(name []byte, root pageNumber) *Collection {
	return &Collection{
		name: name,
		root: root,
	}

}
func newEmptyCollection() *Collection {
	return &Collection{}
}

func (c *Collection) getNodes(indexes []int) ([]*Node, error) {
	root, err := c.tx.getNode(c.root)
	if err != nil {
		return nil, err
	}

	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err = c.tx.getNode(child.childNodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}

func (c *Collection) Find(key []byte) (*Item, error) {
	node, err := c.tx.getNode(c.root)
	if err != nil {
		return nil, err
	}

	index, containingNode, _, err := node.findKey(key, true)

	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, nil
	}
	return containingNode.items[index], nil

}

func (c *Collection) Put(key []byte, value []byte) error {
	if !c.tx.write {
		return writeInsideReadTxErr
	}
	i := NewItem(key, value)

	var root *Node
	var err error
	if c.root == 0 {
		root = c.tx.writeNode(c.tx.newNode([]*Item{i}, []pageNumber{}))
		if err != nil {
			return nil
		}
		c.root = root.pageNum
		return nil
	} else {
		root, err = c.tx.getNode(c.root)
		if err != nil {
			return err
		}
	}

	insertionIndex, nodeToInsertIn, ancestorsIndexes, err := root.findKey(i.key, false)
	fmt.Printf("findkey result: %v  %v  %v  %v\n", insertionIndex, nodeToInsertIn, ancestorsIndexes, err)
	if err != nil {
		return err
	}

	if nodeToInsertIn.items != nil && insertionIndex < len(nodeToInsertIn.items) && bytes.Compare(nodeToInsertIn.items[insertionIndex].key, key) == 0 {
		nodeToInsertIn.items[insertionIndex] = i
	} else {
		nodeToInsertIn.addItem(i, insertionIndex)
	}
	nodeToInsertIn.writeNode(nodeToInsertIn)

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return err
	}
	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]
		if node.isOverPopulated() {
			pnode.split(node, nodeIndex)
		}
	}

	rootNode := ancestors[0]
	if rootNode.isOverPopulated() {
		newRoot := c.tx.newNode([]*Item{}, []pageNumber{rootNode.pageNum})
		newRoot.split(rootNode, 0)
		newRoot = c.tx.writeNode(newRoot)
		if err != nil {
			return err
		}

		c.root = newRoot.pageNum
	}

	return nil
}

func (c *Collection) Remove(key []byte) error {
	if !c.tx.write {
		return writeInsideReadTxErr
	}
	rootNode, err := c.tx.getNode(c.root)
	if err != nil {
		return err
	}

	removeItemIndex, nodeToRemoveFrom, ancestorsIndexes, err := rootNode.findKey(key, true)
	if err != nil {
		return err
	}

	if removeItemIndex == -1 {
		return nil
	}

	if nodeToRemoveFrom.isLeaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex)
	} else {
		affectedNodes, err := nodeToRemoveFrom.removeItemFromInternal(removeItemIndex)
		if err != nil {
			return err
		}
		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return err
	}

	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		if node.isUnderPopulated() {
			err = pnode.rebalanceRemove(node, ancestorsIndexes[i+1])
			if err != nil {
				return err
			}
		}
	}

	rootNode = ancestors[0]
	if len(rootNode.items) == 0 && len(rootNode.childNodes) > 0 {
		c.root = ancestors[1].pageNum
	}

	return nil
}
func (c *Collection) ID() uint64 {
	if !c.tx.write {
		return 0
	}

	id := c.counter
	c.counter += 1
	return id
}

func (c *Collection) serialize() *Item {
	b := make([]byte, collectionSize)
	leftPos := 0
	binary.LittleEndian.PutUint64(b[leftPos:], uint64(c.root))
	leftPos += pageNumberSize
	binary.LittleEndian.PutUint64(b[leftPos:], c.counter)
	leftPos += counterSize
	return NewItem(c.name, b)
}

func (c *Collection) deserialize(item *Item) {
	c.name = item.key

	if len(item.value) != 0 {
		leftPos := 0
		c.root = pageNumber(binary.LittleEndian.Uint64(item.value[leftPos:]))
		leftPos += pageNumberSize

		c.counter = binary.LittleEndian.Uint64(item.value[leftPos:])
		leftPos += pageNumberSize
	}
}
