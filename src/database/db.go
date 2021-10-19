package database

import (
	"github.com/Rorical/NearDB/src/utils"
	"github.com/syndtr/goleveldb/leveldb"
	"sort"
	"strings"
)

type NearDBDatabase struct {
	index *utils.LshForest
	database *leveldb.DB
	datasize int
}

func NewDatabase() (*NearDBDatabase, error) {
	size := 20
	db, err := leveldb.OpenFile("db", nil)
	if err != nil {
		return nil, err
	}
	return &NearDBDatabase{
		index: utils.NewLshForest(size, 6, 3, 3),
		datasize: size,
		database: db,
	}, nil
}

func (db *NearDBDatabase) Add(id string, taglist []string) error {
	err := db.database.Put(utils.StringIn(id), utils.StringIn(strings.Join(taglist, ",")), nil)
	if err != nil {
		return err
	}
	point := utils.CompHash(taglist, db.datasize)
	db.index.Insert(point, id)
	return nil
}

func (db *NearDBDatabase) Refresh() {
	iter := db.database.NewIterator(nil, nil)
	for iter.Next() {
		id := utils.StringOut(iter.Key())
		taglist := strings.Split(utils.StringOut(iter.Value()), ",")
		db.Add(id, taglist)
	}
	iter.Release()
}

func (db *NearDBDatabase) Query(taglist []string, k int) (utils.ItemList, error) {
	point := utils.CompHash(taglist, db.datasize)
	unsortedresult := db.index.Query(point, k)
	originalset := utils.GenSet(taglist)
	itemlist := utils.NewItemList(len(unsortedresult))
	for _, id := range unsortedresult {
		data, _ := db.database.Get(utils.StringIn(id), nil)
		compset := utils.GenSet(strings.Split(utils.StringOut(data), ","))
		distance := utils.CalcDist(originalset, compset)
		itemlist.Add(id, distance)
	}
	sort.Sort(itemlist)
	return itemlist, nil
}

func (db *NearDBDatabase) Close() {
	db.database.Close()
}