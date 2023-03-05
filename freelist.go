package main


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