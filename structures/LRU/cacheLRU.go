package LRU

import (
	"container/list"
	"fmt"
)

type Member struct {
	key   string
	value []byte
}

type CacheLRU struct {
	capacity int
	list     list.List
	hashMap  map[string]*list.Element
}

func (cacheLRU *CacheLRU) SetCapacity(capacity int) {
	cacheLRU.capacity = capacity
}

func New(capacity int) *CacheLRU {

	if capacity <= 0 {
		panic("Greska, nevalidan kapacitet!")
	}

	lru := CacheLRU{}
	lru.capacity = capacity
	lru.list = list.List{}
	lru.hashMap = make(map[string]*list.Element, lru.capacity)
	return &lru
}

func (cacheLRU *CacheLRU) Add(key string, value []byte) {

	el, ok := cacheLRU.hashMap[key]
	if ok {
		newEl := Member{key: key, value: value}
		el.Value = newEl
		cacheLRU.list.MoveToFront(el)
		return
	}

	if cacheLRU.list.Len() == cacheLRU.capacity {
		last := cacheLRU.list.Back()

		cacheLRU.list.Remove(last)
		lastKey := last.Value.(Member).key

		delete(cacheLRU.hashMap, lastKey)
	}

	newEl := Member{key: key, value: value}
	newElement := cacheLRU.list.PushFront(newEl)
	cacheLRU.hashMap[key] = newElement
}

func (cacheLRU *CacheLRU) Get(key string) (bool, []byte) {

	el, ok := cacheLRU.hashMap[key]
	if !ok {
		return false, nil
	}

	cacheLRU.list.MoveToFront(el)

	data := el.Value.(Member).value
	return true, data
}

func (cacheLRU *CacheLRU) Remove(key string) (bool, []byte) {

	el, ok := cacheLRU.hashMap[key]
	if !ok {
		return false, nil
	}

	delete(cacheLRU.hashMap, key)
	cacheLRU.list.Remove(el)

	data := el.Value.(Member).value
	return true, data
}

func main() {
	capacity := 3
	cache := New(capacity)

	key1 := "Key01"
	value1 := []byte("Val01")
	key2 := "Key02"
	value2 := []byte("Val02")
	key3 := "Key03"
	value3 := []byte("Val03")
	key4 := "Key04"
	value4 := []byte("Val04")

	cache.Add(key1, value1)
	cache.Add(key2, value2)
	cache.Add(key3, value3)

	ok, res := cache.Get("Key01") // Will also bump rec1 to the front of the list
	fmt.Println(string(res), ok)

	ok, res = cache.Get("Key09") // exists is now false. Has no effect on order.
	fmt.Println(string(res), ok)

	cache.Add(key4, value4)

	ok, res = cache.Get("Key04")
	fmt.Println(string(res), ok)

	ok, res = cache.Get("Key02")
	fmt.Println(string(res), ok)

	cache.Remove(key4)

}
