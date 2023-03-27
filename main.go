package main

import (
	"fmt"
)

func main() {
	db, _ := Open("minerva.db", &Options{MinFillPercent: 0.5, MaxFillPercent: 1.0})

	tx := db.WriteTx()
	collectionName := "Users"
	createdCollection, _ := tx.CreateCollection([]byte(collectionName))

	newKey := []byte("name")
	newVal := []byte("Celal")
	_ = createdCollection.Put(newKey, newVal)

	_ = tx.Commit()
	_ = db.Close()

	db, _ = Open("minerva.db", &Options{MinFillPercent: 0.5, MaxFillPercent: 1.0})
	tx = db.ReadTx()
	createdCollection, _ = tx.GetCollection([]byte(collectionName))

	item, _ := createdCollection.Find(newKey)

	_ = tx.Commit()
	_ = db.Close()

	fmt.Printf("key is: %s, value is: %s\n", item.key, item.value)
}
