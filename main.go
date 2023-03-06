package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("Os page size:", os.Getpagesize())
	dal, err := newDal("minerva.db")
	if err != nil {
		fmt.Printf("error creating database access layer: %s", err)
	}
	fmt.Println("Database created.")
	newPage := dal.allocateEmptyPage()
	newPage.number = dal.getNextPage()
	copy(newPage.data, "hello from minerva db")

	if err := dal.writePage(newPage); err != nil {
		fmt.Printf("error when writing page: %s", err)
	}
	if _,err:=dal.writeFreeList(); err != nil {
		fmt.Printf("error when writing freelist: %s", err)
	}
	fmt.Println("free list written successfully!")

	if err:=dal.close(); err != nil {
		fmt.Printf("error when writing freelist: %s", err)
	}
	fmt.Println("database closed.")

	dal, _= newDal("minerva.db")
	newPage = dal.allocateEmptyPage()
	newPage.number = dal.getNextPage()
	copy(newPage.data, "The owl of Minerva spreads its wings only with the falling of the dusk.")
	err = dal.writePage(newPage)
	pageNumber := dal.getNextPage()
	dal.releasePage(pageNumber)
	_, _ = dal.writeFreeList()

}
