package skipList

import (
	"fmt"
	"math/rand"
	"napredni/structures/record"
)

type Element struct {
	Level int
	Rec       record.Record
	Next      []*Element
	Prev      []*Element
	Tombstone bool
}

type SkipList struct {
	Begin    Element
	Elements []Element
}

func (sl *SkipList) PrintSL() {

	for i := len(sl.Begin.Next) - 1; i >= 0; i-- {
		fmt.Print(sl.Begin.Next[i])
		fmt.Print("\t")
		for j := 0; j < len(sl.Elements); j++ {
			if len(sl.Elements[j].Next) <= i {
				fmt.Print("\t/\t")
			} else {
				if sl.Elements[j].Next[i] == nil {
					fmt.Print("\tnull\t")
				} else {
					fmt.Print(sl.Elements[j].Next[i])
					fmt.Print("\t")
				}
			}
		}
		fmt.Print("\n")
	}
	fmt.Print("/\t")
	for j := 0; j < len(sl.Elements); j++ {
		fmt.Print(sl.Elements[j].Rec.Key)
		fmt.Print("\t")
	}}

func (sl *SkipList) AddEl(r record.Record, tombstone bool) {
	var found, path = sl.FindEl(r.Key)
	if found {
		var el = path[len(path)-1]
		el.Rec.Value = r.Value
		el.Tombstone = false
		return
	}

	//refill
	for {
		if len(path) < len(sl.Begin.Next) {
			path = append(path, path[len(path)-1])
		} else {
			break
		}
	}

	//reverse
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	var Level = calcLevel()
	var max = len(sl.Begin.Next)
	var el = Element{
		Level:     Level,
		Rec:       r,
		Next:      nil,
		Prev:      nil,
		Tombstone: tombstone,
	}

	for i := 0; i < min(max, Level); i++ {
		el.Next = append(el.Next, path[i].Next[i])
		el.Prev = append(el.Prev, path[i])
		if path[i].Next[i] != nil {
			path[i].Next[i].Prev[i] = &el
		}
		path[i].Next[i] = &el
	}
	for i := max; i < Level; i++ {
		el.Next = append(el.Next, nil)
		el.Prev = append(el.Prev, &sl.Begin)
		sl.Begin.Next = append(sl.Begin.Next, &el)
	}
	sl.Begin.Level = len(sl.Begin.Next)
	sl.Elements = append(sl.Elements, el)
}

func (sl *SkipList) UpdateEl(r record.Record) {
	sl.AddEl(r, false)
}

func (sl *SkipList) FindEl(k string) (found bool, path []*Element) {

	if len(sl.Elements) == 0 {
		return false, nil
	}
	var current = &sl.Begin
	for i := len(sl.Begin.Next) - 1; i >= 0; i-- {
		for {
			if current.Next[i] != nil {
				if current.Next[i].Rec.Key == k {
					path = append(path, current)
					current = current.Next[i]
					path = append(path, current)
					return true, path
				}
				if current.Next[i].Rec.Key > k {
					break
				}
				current = current.Next[i]
			} else {
				break
			}
		}
		path = append(path, current)
	}
	return false, path
}

func (sl *SkipList) DeleteEl(r record.Record) bool {
	var found, path = sl.FindEl(r.Key)
	if !found {
		sl.AddEl(r, true)
		return false
	}
	var el = path[len(path)-1]
	el.Tombstone = true
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func calcLevel() (Level int) {
	Level = 1
	var x = rand.Intn(2)
	for {
		if x == 1 {
			Level += 1
			x = rand.Intn(2)
		} else {
			break
		}
	}
	return Level
}