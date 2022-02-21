package main

import (
	"bufio"
	"fmt"
	"napredni/menu"
	"napredni/structures/CMS"
	"napredni/structures/HLL"
	"napredni/structures/LRU"
	"napredni/structures/LSM"
	"napredni/structures/WAL"
	"napredni/structures/configReader"
	"napredni/structures/readPath"
	"napredni/structures/record"
	"napredni/structures/writePath"
	"os"
	"time"
)

func WALToMemtable(wal *WAL.WAL, lsm *LSM.LSM) {
	for _, segmentPath := range wal.SegmentPaths {
		file, err := os.OpenFile(segmentPath, os.O_RDWR, 0777)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		reader := bufio.NewReader(file)
		rec := record.Record{}
		for {
			if rec.DecodeRecord(reader) {
				break
			}

			if rec.Crc == WAL.CRC32(rec.Value) {
				var _, _, _ = lsm.MemTable.AddRecord(rec)
			} else {
				panic("CRC is not compatible!")
			}
		}
	}
}

func App() {
	var config configReader.Config
	config.ReadConfig()

	mem := writePath.InitializeMemTable(float64(config.SegmentSize), config.MemtableThreshold)
	lsm := writePath.InitializeLSM(mem, config.LsmLevels, config.LsmLevelMax)
	lsm.UpdateLSM()

	wal := writePath.InitializeWAL(config.Lwm, config.SegmentSize)
	tb := writePath.InitializeTokenBucket(config.TokenTime, config.TokenRequests)
	WALToMemtable(wal, lsm)
	cache := LRU.New(config.CacheSize)

	hll := HLL.HLL{}
	cms := CMS.CountMinSketch{}

	reader := bufio.NewReader(os.Stdin)
	i := 0
	for {
		menu.PrintMenu()
		command := menu.GetInputFromUser("Odaberite operaciju: ", reader)

		if command == "1" {
			key, value := menu.GetKeyAndValueFromUser(reader)
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				writePath.Put(wal, lsm, key, value)
				fmt.Println("Uspešan zahtev.")
			} else {
				fmt.Println("Neuspešan zahtev.")
			}

		} else if command == "2" {
			key := menu.GetInputFromUser("Unesite ključ elementa koji brišete: ", reader)
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				success := writePath.Delete(wal, cache, lsm, key)
				fmt.Println(success)
				fmt.Println("Uspešan zahtev.")
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "3" {
			key := menu.GetInputFromUser("Unesite ključ elementa koji pretražujete: ", reader)
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				found, bytes := readPath.Get(cache, lsm, key)
				fmt.Println("Uspešan zahtev.")

				if found {
					fmt.Println("Ključ:", key+", Vrednost:", string(bytes))
				} else {
					fmt.Println("Nije pronađen element za uneti ključ.")
				}
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "4" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				menu.FirstMenuCMS(cms, wal, lsm, cache, tb, i)
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "5" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				menu.FirstMenuHLL(hll, wal, lsm, cache, tb, i)
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "6" {
			break
		} else {
			fmt.Println("Pogrešan unos.")
		}
	}
}

func main() {
	App()
}
