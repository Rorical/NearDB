package utils

import (
	mapset "github.com/deckarep/golang-set"
)

type Item struct {
	Id string
	Distance float32
}

type ItemList []Item

func NewItemList(len int) ItemList {
	return make(ItemList, 0, len)
}

func (sl *ItemList) Add(id string, distance float32) {
	*sl = append(*sl, Item{
		Id: id,
		Distance: distance,
	})
}

func (sl ItemList) Len() int {
	return len(sl)
}

func (sl ItemList) Less(i, j int) bool {
	return sl[i].Distance > sl[j].Distance
}

func (sl ItemList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

func CalcDist(set1, set2 mapset.Set) float32 {
	dif := float32(set1.Intersect(set2).Cardinality()) / float32(set1.Union(set2).Cardinality())
	return dif
}

func GenSet(s []string) mapset.Set {
	set := mapset.NewSet()
	for _, t := range s {
		set.Add(t)
	}
	return set
}