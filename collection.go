package main

import (
	"bytes"
	"fmt"
)

type Collection struct {
	name []byte
	root pageNumber
	dal  *dal
}

func NewCollection(name []byte, root pageNumber) *Collection {
	return &Collection{
		name: name,
		root: root,
	}

}

func (c *Collection) getNodes(indexes []int) ([]*Node, error) {
	root, err := c.dal.getNode(c.root)
	if err != nil {
		return nil, err
	}

	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err = c.dal.getNode(child.childNodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}

func (c *Collection) Find(key []byte) (*Item, error) {
	node, err := c.dal.getNode(c.root)
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
	i := NewItem(key, value)

	var root *Node
	var err error
	if c.root == 0 {
		root, err = c.dal.writeNode(c.dal.newNode([]*Item{i}, []pageNumber{}))
		if err != nil {
			return nil
		}
		c.root = root.pageNum
		return nil
	} else {
		root, err = c.dal.getNode(c.root)
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
		newRoot := c.dal.newNode([]*Item{}, []pageNumber{rootNode.pageNum})
		newRoot.split(rootNode, 0)
		newRoot, err = c.dal.writeNode(newRoot)
		if err != nil {
			return err
		}

		c.root = newRoot.pageNum
	}

	return nil
}
