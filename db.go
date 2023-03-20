package main

import (
	"os"
	"sync"
)

type DB struct {
	rwlock sync.RWMutex
	*dal
}


func Open(path string, options *Options) (*DB, error){
	var err error

	options.pageSize = os.Getpagesize()
	dal,err := newDal(path, options)
	if err !=nil {
		return nil,err
	}
	db:=&DB{
		sync.RWMutex{},
		dal,
	}

	return db, nil
}


func (db *DB) Close() error {
	return db.close()
}

func (db *DB) ReadTx() *Tx {
	db.rwlock.RLock()
	return NewTx(db, false)
}
func (db *DB) WriteTx() *Tx {
	db.rwlock.Lock()
	return NewTx(db, true)
}

