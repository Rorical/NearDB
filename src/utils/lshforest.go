package utils

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// https://github.com/ekzhu/lsh/blob/master/lsh.go

const (
	rand_seed = 1
)

type Point []float64

// Dot returns the dot product of two points.
func (p Point) Dot(q Point) float64 {
	s := 0.0
	for i := 0; i < len(p); i++ {
		s += p[i] * q[i]
	}
	return s
}

// L2 returns the L2 distance of two points.
func (p Point) L2(q Point) float64 {
	s := 0.0
	for i := 0; i < len(p); i++ {
		d := p[i] - q[i]
		s += d * d
	}
	return math.Sqrt(s)
}

// Key is a way to index into a table.
type hashTableKey []int

// Value is an index into the input dataset.
type hashTableBucket []string

type lshParams struct {
	// Dimensionality of the input data.
	dim int
	// Number of hash tables.
	l int
	// Number of hash functions for each table.
	m int
	// Shared constant for each table.
	w float64

	// Hash function params for each (l, m).
	a [][]Point
	b [][]float64
}

// NewLshParams initializes the LSH settings.
func newLshParams(dim, l, m int, w float64) *lshParams {
	// Initialize hash params.
	a := make([][]Point, l)
	b := make([][]float64, l)
	random := rand.New(rand.NewSource(rand_seed))
	for i := range a {
		a[i] = make([]Point, m)
		b[i] = make([]float64, m)
		for j := range a[i] {
			a[i][j] = make(Point, dim)
			for d := 0; d < dim; d++ {
				a[i][j][d] = random.NormFloat64()
			}
			b[i][j] = random.Float64() * float64(w)
		}
	}
	return &lshParams{
		dim: dim,
		l:   l,
		m:   m,
		a:   a,
		b:   b,
		w:   w,
	}
}

// Hash returns all combined hash values for all hash tables.
func (lsh *lshParams) hash(point Point) []hashTableKey {
	hvs := make([]hashTableKey, lsh.l)
	for i := range hvs {
		s := make(hashTableKey, lsh.m)
		for j := 0; j < lsh.m; j++ {
			hv := (point.Dot(lsh.a[i][j]) + lsh.b[i][j]) / lsh.w
			s[j] = int(math.Floor(hv))
		}
		hvs[i] = s
	}
	return hvs
}

// LshForest implements the LSH Forest algorithm by Mayank Bawa et.al.
// It supports both nearest neighbour candidate query and k-NN query.
type LshForest struct {
	// Embedded type
	*lshParams
	// Trees.
	tables []*leveldb.DB
}

// NewLshForest creates a new LSH Forest for L2 distance.
// dim is the diminsionality of the data, l is the number of hash
// tables to use, m is the number of hash values to concatenate to
// form the key to the hash tables, w is the slot size for the
// family of LSH functions.
func NewLshForest(dim, l, m int, w float64) *LshForest {
	tables := make([]*leveldb.DB, l)
	var err error
	for i := range tables {
		tables[i], err = leveldb.OpenFile(fmt.Sprintf("table_%d", i), nil)
		if err != nil {
			panic(err)
		}
	}
	return &LshForest{
		lshParams: newLshParams(dim, l, m, w),
		tables:     tables,
	}
}

func ListExist(list []string, item string) bool {
	for _, it := range list {
		if it == item {
			return true
		}
	}
	return false
}

func ListAdd(db *leveldb.DB, key []byte, item string) error {
	val, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	items := strings.Split(StringOut(val), ",")
	if !ListExist(items, item) {
		items = append(items, item)
	}
	return db.Put(key, StringIn(strings.Join(items, ",")), &opt.WriteOptions{Sync: true})
}

func DeleteSlice(list []string, ele string) []string{
	i := 0
	for i = range list {
		if list[i] == ele {
			break
		}
	}
	return list[:i+copy(list[i:], list[i+1:])]
}

func ListDelete(db *leveldb.DB, key []byte, item string) error {
	val, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	items := strings.Split(StringOut(val), ",")
	if ListExist(items, item) {
		items = DeleteSlice(items, item)
	}
	return db.Put(key, StringIn(strings.Join(items, ",")), &opt.WriteOptions{Sync: true})
}


// Insert adds a new data point to the LSH Forest.
// id is the unique identifier for the data point.
func (index *LshForest) Insert(point Point, id string) {
	// Apply hash functions.
	hvs := index.hash(point)
	// Parallel insert
	var wg sync.WaitGroup
	wg.Add(len(index.tables))
	for i := range index.tables {
		hv := hvs[i]
		table := index.tables[i]
		go func(table *leveldb.DB, hv hashTableKey) {
			key := IntsToBytes(hv)
			if i, e := table.Has(key, nil); i && e == nil {
				if e := ListAdd(table, key, id); e != nil {
					panic(e)
				}
			} else {
				if e := table.Put(key, StringIn(id), &opt.WriteOptions{Sync: true}); e != nil {
					panic(e)
				}
			}
			wg.Done()
		}(table, hv)
	}
	wg.Wait()
}

func (index *LshForest) Delete(point Point, id string) {
	hvs := index.hash(point)
	var wg sync.WaitGroup
	wg.Add(len(index.tables))
	for i := range index.tables {
		hv := hvs[i]
		table := index.tables[i]
		go func(table *leveldb.DB, hv hashTableKey) {
			key := IntsToBytes(hv)
			if i, e := table.Has(key, nil); i && e == nil {
				if e := ListDelete(table, key, id); e != nil {
					panic(e)
				}
			}
			wg.Done()
		}(table, hv)
	}
	wg.Wait()
}

func lookup(table *leveldb.DB, maxLevel int, tableKey hashTableKey, done <-chan interface{}, out chan<- string) {
	iter := table.NewIterator(util.BytesPrefix(IntsToBytes(tableKey[:maxLevel])), nil)
	for iter.Next() {
		items := strings.Split(StringOut(iter.Value()), ",")
		for _, item := range items {
			temp := make([]byte, len(item))
			copy(temp, item)
			select {
			case out <- StringOut(temp):
			case <-done:
				return
			}
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		panic(err)
	}
}

// Helper that queries all trees and returns an channel ids.
func (index *LshForest) queryHelper(maxLevel int, tableKeys []hashTableKey, done <-chan interface{}, out chan<- string) {
	var wg sync.WaitGroup
	wg.Add(len(index.tables))
	for i := range index.tables {
		key := tableKeys[i]
		table := index.tables[i]
		go func(table *leveldb.DB, hv hashTableKey) {
			lookup(table, maxLevel, hv, done, out)
			wg.Done()
		}(table, key)
	}
	wg.Wait()
}

// Query finds at top-k ids of approximate nearest neighbour candidates,
// in unsorted order, given the query point.
func (index *LshForest) Query(q Point, k int) []string {
	// Apply hash functions
	hvs := index.hash(q)
	// Query
	results := make(chan string)
	done := make(chan interface{})
	go func() {
		for maxLevels := index.m; maxLevels >= 0; maxLevels-- {
			select {
			case <-done:
				return
			default:
				index.queryHelper(maxLevels, hvs, done, results)
			}
		}
		close(results)
	}()
	seen := make(map[string]bool)
L:	for {
		select {
		case id, ok:=<-results:
			if ok {
				if len(seen) >= k {
					break L
				}
				if _, exist := seen[id]; exist {
					continue
				}

				seen[id] = true
			} else {
				break L
			}
		case <-time.After(5 * time.Second):
			break L
		default:
			if len(seen) >= k {
				break L
			}
		}
	}
	for i := index.m + 2; i >= 0 ; i -- {
		select {
		case done <- nil:
		default:
			continue
		}
	}

	close(done)
	// Collect results
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}


func (index *LshForest) Close() error {
	var err error
	for _, d := range index.tables {
		err = d.Close()
		if err != nil {
			return err
		}
	}
	return nil
}