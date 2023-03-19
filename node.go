package main

import (
	"bytes"
	"encoding/binary"
)

type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	*dal

	pageNum    pageNumber
	items      []*Item
	childNodes []pageNumber
}

func NewNode() *Node {
	return &Node{}
}

func NewNodeForSerialization(items []*Item, childNodes []pageNumber) *Node {
	return &Node{
		items:      items,
		childNodes: childNodes,
	}
}

func NewItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func isLast(index int, parentNode *Node) bool {
	return index == len(parentNode.items)
}

func isFirst(index int) bool {
	return index == 0
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) writeNode(node *Node) *Node {
	node, _ = n.dal.writeNode(node)
	return node
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pageNum pageNumber) (*Node, error) {
	return n.dal.getNode(pageNum)
}

func (n *Node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

func (n *Node) canSpareAnElement() bool {
	splitIndex := n.dal.getSplitIndex(n)
	if splitIndex == -1 {
		return false
	}
	return true
}

func (n *Node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isLeaf {
			childNode := n.childNodes[i]

			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumberSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)

		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isLeaf {
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftPos := 0
	isLeaf := uint16(buf[0])

	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	for i := 0; i < itemsCount; i++ {
		if isLeaf == 0 { 
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumberSize

			n.childNodes = append(n.childNodes, pageNumber(pageNum))
		}

		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen
		n.items = append(n.items, NewItem(key, value))
	}

	if isLeaf == 0 { 
		pageNum := pageNumber(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}

func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumberSize 
	return size
}
func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.items {
		size += n.elementSize(i)
	}

	size += pageNumberSize 
	return size
}


func (n *Node) findKey(key []byte, exact bool) (int, *Node, []int ,error) {
	ancestorsIndexes := []int{0} 
	index, node, err := findKeyHelper(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, ancestorsIndexes, nil
}

func findKeyHelper(node *Node, key []byte, exact bool, ancestorsIndexes *[]int) (int, *Node ,error) {
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key, exact, ancestorsIndexes)
}
func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.items {
		res := bytes.Compare(existingItem.key, key)
		if res == 0 { 
			return true, i
		}

		if res == 1 {
			return false, i
		}
	}

	return false, len(n.items)
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex { 
		n.items = append(n.items, item)
		return insertionIndex
	}

	n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
	n.items[insertionIndex] = item
	return insertionIndex
}


func (n *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	splitIndex := nodeToSplit.dal.getSplitIndex(nodeToSplit)
	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf() {
		newNode = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], []pageNumber{}))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}
	n.addItem(middleItem, nodeToSplitIndex)
	if len(n.childNodes) == nodeToSplitIndex+1 { 
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNum
	}

	n.writeNodes(n, nodeToSplit)
}


func (n *Node) removeItemFromLeaf(index int) {
	n.items = append(n.items[:index], n.items[index+1:]...)
	n.writeNode(n)
}

func (n *Node) removeItemFromInternal(index int) ([]int, error) {
		affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)
	aNode, err := n.getNode(n.childNodes[index])
	if err != nil {
		return nil, err
	}

	for !aNode.isLeaf() {
		traversingIndex := len(n.childNodes) - 1
		aNode, err = n.getNode(n.childNodes[traversingIndex])
		if err != nil {
			return nil, err
		}
		affectedNodes = append(affectedNodes, traversingIndex)
	}
	n.items[index] = aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]
	n.writeNodes(n, aNode)
	return affectedNodes, nil
}


func rotateRight(aNode, pNode, bNode *Node, bNodeIndex int) {
	aNodeItem := aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	pNodeItemIndex := bNodeIndex - 1
	if isFirst(bNodeIndex) {
		pNodeItemIndex = 0
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = aNodeItem

	bNode.items = append([]*Item{pNodeItem}, bNode.items...)

	if !aNode.isLeaf() {
		childNodeToShift := aNode.childNodes[len(aNode.childNodes)-1]
		aNode.childNodes = aNode.childNodes[:len(aNode.childNodes)-1]
		bNode.childNodes = append([]pageNumber{childNodeToShift}, bNode.childNodes...)
	}
}

func rotateLeft(aNode, pNode, bNode *Node, bNodeIndex int) {
	bNodeItem := bNode.items[0]
	bNode.items = bNode.items[1:]

	pNodeItemIndex := bNodeIndex
	if isLast(bNodeIndex, pNode) {
		pNodeItemIndex = len(pNode.items) - 1
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = bNodeItem

	aNode.items = append(aNode.items, pNodeItem)

	if !bNode.isLeaf() {
		childNodeToShift := bNode.childNodes[0]
		bNode.childNodes = bNode.childNodes[1:]
		aNode.childNodes = append(aNode.childNodes, childNodeToShift)
	}
}

func (n *Node) merge(bNode *Node, bNodeIndex int) error {
	aNode, err := n.getNode(n.childNodes[bNodeIndex-1])
	if err != nil {
		return err
	}

	pNodeItem := n.items[bNodeIndex-1]
	n.items = append(n.items[:bNodeIndex-1], n.items[bNodeIndex:]...)
	aNode.items = append(aNode.items, pNodeItem)

	aNode.items = append(aNode.items, bNode.items...)
	n.childNodes = append(n.childNodes[:bNodeIndex], n.childNodes[bNodeIndex+1:]...)
	if !aNode.isLeaf() {
		aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
	}
	n.writeNodes(aNode, n)
	n.dal.deleteNode(bNode.pageNum)
	return nil
}


func (n *Node) rebalanceRemove(unbalancedNode *Node, unbalancedNodeIndex int) error {
	pNode := n

	if unbalancedNodeIndex != 0 {
		leftNode, err := n.getNode(pNode.childNodes[unbalancedNodeIndex-1])
		if err != nil {
			return err
		}
		if leftNode.canSpareAnElement() {
			rotateRight(leftNode, pNode, unbalancedNode, unbalancedNodeIndex)
			n.writeNodes(leftNode, pNode, unbalancedNode)
			return nil
		}
	}

	if unbalancedNodeIndex != len(pNode.childNodes)-1 {
		rightNode, err := n.getNode(pNode.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return err
		}
		if rightNode.canSpareAnElement() {
			rotateLeft(unbalancedNode, pNode, rightNode, unbalancedNodeIndex)
			n.writeNodes(unbalancedNode, pNode, rightNode)
			return nil
		}
	}
	if unbalancedNodeIndex == 0 {
		rightNode, err := n.getNode(n.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return err
		}

		return pNode.merge(rightNode, unbalancedNodeIndex+1)
	}

	return pNode.merge(unbalancedNode, unbalancedNodeIndex)
}