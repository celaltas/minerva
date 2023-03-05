package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("Os page size:", os.Getpagesize())
	dal, err := newDal("minerva.db", os.Getpagesize())
	if err != nil {
		fmt.Printf("error creating database access layer: %s", err)
	}
	newPage := dal.allocateEmptyPage()
	newPage.number = dal.freeList.getNextPage()
	copy(newPage.data, "hello from minerva db")

	if err := dal.writePage(newPage); err != nil {
		fmt.Printf("error when writing page: %s", err)
	}
}
