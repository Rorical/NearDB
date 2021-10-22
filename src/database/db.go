package database

import (
	"errors"
	"fmt"
	"github.com/Rorical/NearDB/src/utils"
	"github.com/hashicorp/golang-lru"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"sort"
	"strings"
	"sync"
)

type NearDBDatabase struct {
	index     *utils.LshForest
	database  *leveldb.DB
	dblock    *sync.RWMutex
	indexlock *sync.RWMutex
	cache     *lru.Cache
	datasize  int
}


func NewDatabase() (*NearDBDatabase, error) {
	size := 20
	log.Println("Loading DB")
	db, err := leveldb.OpenFile("db", nil)
	if err != nil {
		return nil, err
	}
	cache, err := lru.New(100)
	if err != nil {
		return nil, err
	}
	log.Println("DB Loaded")
	return &NearDBDatabase{
		index:    utils.NewLshForest(size, 6, 3, 3),
		datasize: size,
		database: db,
		cache: cache,
		dblock: &sync.RWMutex{},
		indexlock: &sync.RWMutex{},
	}, nil
}

func (db *NearDBDatabase) Add(id string, set []string) error {
	db.dblock.Lock()
	err := db.database.Put(utils.StringIn(id), utils.StringIn(strings.Join(set, ",")), &opt.WriteOptions{Sync: true})
	db.dblock.Unlock()
	if err != nil {
		return err
	}
	point := utils.CompHash(set, db.datasize)
	db.indexlock.Lock()
	db.index.Insert(point, id)
	db.indexlock.Unlock()
	return nil
}

func (db *NearDBDatabase) Remove(id string) error {
	db.dblock.RLock()
	data, err := db.database.Get(utils.StringIn(id), nil)
	db.dblock.RUnlock()
	if err != nil {
		return err
	}
	set := strings.Split(utils.StringOut(data), ",")
	point := utils.CompHash(set, db.datasize)
	db.indexlock.Lock()
	db.index.Delete(point, id)
	db.indexlock.Unlock()
	return nil
}

func (db *NearDBDatabase) Refresh() {
	db.dblock.RLock()
	defer db.dblock.RUnlock()
	iter := db.database.NewIterator(nil, nil)
	for iter.Next() {
		id := utils.StringOut(iter.Key())
		set := strings.Split(utils.StringOut(iter.Value()), ",")
		db.Add(id, set)
	}
	iter.Release()
}

func (db *NearDBDatabase) Query(set []string, k int) (utils.ItemList, error) {
	point := utils.CompHash(set, db.datasize)
	if val, exist := db.cache.Get(utils.PointInfo(point, k)); exist {
		return val.(utils.ItemList), nil
	}
	db.indexlock.RLock()
	unsortedresult := db.index.Query(point, k)
	db.indexlock.RUnlock()
	originalset := utils.GenSet(set)
	itemlist := utils.NewItemList(len(unsortedresult))
	for _, id := range unsortedresult {
		db.dblock.RLock()
		data, _ := db.database.Get(utils.StringIn(id), nil)
		db.dblock.RUnlock()
		compset := utils.GenSet(strings.Split(utils.StringOut(data), ","))
		distance := utils.CalcDist(originalset, compset)
		itemlist.Add(id, distance)
	}
	sort.Sort(itemlist)
	db.cache.Add(utils.PointInfo(point, k), itemlist)
	return itemlist, nil
}

func (db *NearDBDatabase) QueryPage(set []string, k, offset, all int) (utils.ItemList, error) {
	log.Printf("Querying Set %v\n", set)
	log.Printf("Limit: %d, Offset: %d, All: %d", k, offset, all)
	point := utils.CompHash(set, db.datasize)
	if val, exist := db.cache.Get(utils.PointInfo(point, all)); exist {
		list := val.(utils.ItemList)
		fmt.Printf("%v\n", list)
		if offset + k < len(list) {
			return list[offset:offset + k], nil
		}
		return nil, errors.New("Offset Exceed All")
	}
	db.indexlock.RLock()
	unsortedresult := db.index.Query(point, all)
	db.indexlock.RUnlock()
	originalset := utils.GenSet(set)
	itemlist := utils.NewItemList(len(unsortedresult))
	for _, id := range unsortedresult {
		db.dblock.RLock()
		data, _ := db.database.Get(utils.StringIn(id), nil)
		db.dblock.RUnlock()
		compset := utils.GenSet(strings.Split(utils.StringOut(data), ","))
		distance := utils.CalcDist(originalset, compset)
		itemlist.Add(id, distance)
	}
	sort.Sort(itemlist)
	db.cache.Add(utils.PointInfo(point, all), itemlist)
	return itemlist[offset:offset + k], nil
}

func (db *NearDBDatabase) Close() {
	db.database.Close()
	db.index.Close()
}
