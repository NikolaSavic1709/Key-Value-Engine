package Memtable

import (
	"napredni/structures/record"
	"napredni/structures/skipList"
)

type MemTable struct {
	Capacity  float64
	Threshold float64
	Sl        *skipList.SkipList
	rec       []record.Record
}

func (mt *MemTable) FillDefaults() {
	if mt.Capacity == 0 {
		mt.Capacity = 10
	}
	if mt.Threshold == 0 {
		mt.Threshold = 0.8
	}
	var el0 = skipList.Element{
		Level: 0,
		Rec: record.Record{
			Crc:       0,
			Timestamp: 0,
			Tombstone: 0,
			KeySize:   0,
			ValueSize: 0,
			Key:       "",
			Value:     nil,
		},
		Next:      nil,
		Prev:      nil,
		Tombstone: false,
	}
	mt.Sl = &skipList.SkipList{Begin: el0}
	mt.rec = nil

}

func (mt *MemTable) Empty() {
	mt.FillDefaults()
}

func (mt *MemTable) AddRecord(r record.Record) ([]record.Record, bool, bool) {
	var records []record.Record = nil
	found := false
	if r.Tombstone == 1 {
		found, _ = mt.Sl.FindEl(r.Key)
		if !found {
			if mt.Sl != nil && len(mt.Sl.Elements) >= int(mt.Capacity*mt.Threshold) {
				records = mt.Flush()
				mt.Empty()
			}
			found = mt.Sl.DeleteEl(r)
		} else{
			found = mt.Sl.DeleteEl(r)
		}
	} else {
		found, _ = mt.Sl.FindEl(r.Key)
		if found {
			mt.Sl.AddEl(r, false)
		} else {
			if mt.Sl != nil && len(mt.Sl.Elements) >= int(mt.Capacity*mt.Threshold) {
				records = mt.Flush()
				mt.Empty()
			}
			mt.Sl.AddEl(r, false)
		}
	}
	return records, true, found
}

func (mt *MemTable) GetRecord(key string) (bool, []byte) {
	found, path := mt.Sl.FindEl(key)
	if found {
		el := path[len(path)-1]
		if !el.Tombstone {
			return true, el.Rec.Value
		} else {
			return false, el.Rec.Value
		}
	}
	return false, nil
}

func (mt *MemTable) Fill(r []record.Record) []record.Record {
	for i := 0; i < len(r); i++ {
		if len(mt.Sl.Elements) == int(mt.Capacity*mt.Threshold) {
			records := mt.Flush()
			mt.Empty()
			return records
		}
		if r[i].Tombstone == 1 {
			mt.Sl.DeleteEl(r[i])
		} else {
			mt.Sl.AddEl(r[i], false)
		}
	}
	return nil
}

func (mt *MemTable) Flush() []record.Record {
	if mt.Sl.Elements == nil {
		return nil
	}
	var r []record.Record
	var current = mt.Sl.Begin.Next[0]
	for {
		if current == nil {
			break
		}
		r = append(r, current.Rec)
		current = current.Next[0]
	}
	return r
}
