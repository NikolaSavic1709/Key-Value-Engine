package menu

import (
	"bufio"
	"fmt"
	"napredni/structures/CMS"
	"napredni/structures/HLL"
	"napredni/structures/LRU"
	"napredni/structures/LSM"
	"napredni/structures/WAL"
	"napredni/structures/readPath"
	"napredni/structures/tokenBucket"
	"napredni/structures/writePath"
	"os"
	"strings"
	"time"
)

func PrintMenu() {
	fmt.Println()
	fmt.Println("**************************Meni**************************")
	fmt.Println("1 - Dodavanje parova ključ - vrednost u bazu podataka.")
	fmt.Println("2 - Brisanje elementa iz baze podataka.")
	fmt.Println("3 - Pretraga baze podataka za uneti ključ.")
	fmt.Println("4 - CMS funkcionalnosti")
	fmt.Println("5 - HLL funkcionalnosti")
	fmt.Println("6 - Izlazak iz programa.")
}

func PrintMenuHLL() {
	fmt.Println()
	fmt.Println("**************************Meni HLL**************************")
	fmt.Println("1 - Dodavanje novog HLL-a u bazu podataka")
	fmt.Println("2 - Brisanje HLL-a iz baze podataka")
	fmt.Println("3 - Pretraga baze podataka i rukovanje za uneti ključ")
	fmt.Println("4 - Korak nazad")
}

func PrintMenuCMS() {
	fmt.Println()
	fmt.Println("**************************Meni CMS**************************")
	fmt.Println("1 - Dodavanje novog CMS-a u bazu podataka")
	fmt.Println("2 - Brisanje CMS-a iz baze podataka")
	fmt.Println("3 - Pretraga baze podataka i rukovanje za uneti ključ")
	fmt.Println("4 - Korak nazad")
}

func HandlerHLL() {
	fmt.Println()
	fmt.Println("**************************Meni HLL**************************")
	fmt.Println("1 - Dodavanje elementa")
	fmt.Println("2 - Prikaz kardinalnosti elemenata")
	fmt.Println("3 - Korak nazad")
}

func HandlerCMS() {
	fmt.Println()
	fmt.Println("**************************Meni CMS**************************")
	fmt.Println("1 - Dodavanje elementa")
	fmt.Println("2 - Prikaz frekvencije elementa")
	fmt.Println("3 - Korak nazad")
}

func FirstMenuCMS(cms CMS.CountMinSketch, wal *WAL.WAL, lsm *LSM.LSM, cache *LRU.CacheLRU, tb *tokenBucket.TokenBucket, i int) {
	reader := bufio.NewReader(os.Stdin)

	for {
		PrintMenuCMS()
		command := GetInputFromUser("Odaberite operaciju: ", reader)
		if command == "4" {
			break
		}
		key := GetInputFromUser("Unesite kljuc CMS-a kojim zelite da rukujete: ", reader)
		key = "cms_" + key
		if command == "1" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				cms = *CMS.CreateCountMinSketch(0.01, 0.01)
				value := cms.DecodeCMS()
				writePath.Put(wal, lsm, key, value)
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "2" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				succes := writePath.Delete(wal, cache, lsm, key)
				if succes {
					fmt.Println("Uspesno obrisan CMS")
				} else {
					fmt.Println("Ne postoji CMS sa ovakvim kljucem")
				}
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "3" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				found, cmsBytes := readPath.Get(cache, lsm, key)
				if found {
					cms.EncodeCMS(cmsBytes)
					SecondMenuCMS(cms, wal, lsm, key)
				} else {
					fmt.Println("Ne postoji CMS sa ovim kljucem")
				}
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		}
	}
}

func FirstMenuHLL(hll HLL.HLL, wal *WAL.WAL, lsm *LSM.LSM, cache *LRU.CacheLRU, tb *tokenBucket.TokenBucket, i int) {
	reader := bufio.NewReader(os.Stdin)

	for {
		PrintMenuHLL()
		command := GetInputFromUser("Odaberite operaciju: ", reader)
		if command == "4" {
			break
		}
		key := GetInputFromUser("Unesite kljuc HLL-a kojim zelite da rukujete: ", reader)
		key = "hll_" + key
		if command == "1" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				hll = *HLL.CreateHLL(4)
				value := hll.DecodeHLL()
				writePath.Put(wal, lsm, key, value)
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "2" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				succes := writePath.Delete(wal, cache, lsm, key)
				if succes {
					fmt.Println("Uspesno obrisan HLL")
				} else {
					fmt.Println("Ne postoji HLL sa ovakvim kljucem")
				}
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		} else if command == "3" {
			if i == 0 {
				tb.Start = time.Now()
				tb.AvailableRequests--
				i = 1
			}
			if tb.Handler() {
				fmt.Println("Uspešan zahtev.")
				found, hllBytes := readPath.Get(cache, lsm, key)
				if found {
					hll.EncodeHLL(hllBytes)
					SecondMenuHLL(hll, wal, lsm, key)
				} else {
					fmt.Println("Ne postoji HLL sa ovim kljucem")
				}
			} else {
				fmt.Println("Neuspešan zahtev.")
			}
		}
	}
}

func SecondMenuCMS(cms CMS.CountMinSketch, wal *WAL.WAL, lsm *LSM.LSM, key string) {
	reader := bufio.NewReader(os.Stdin)

	for {
		HandlerCMS()
		command := GetInputFromUser("Odaberite operaciju: ", reader)
		if command == "1" {
			element := GetInputFromUser("Dodajte elemente koji želite: ", reader)
			cms.AddData(element)
		} else if command == "2" {
			element := GetInputFromUser("Unesite element čiju frekvenciju želite za prikaz ", reader)
			fmt.Print("Frekvencija elementa: " + "'" + element + "': ")
			fmt.Println(cms.FindDataFrequency(element))
		} else if command == "3" {
			break
		}
	}
	value := cms.DecodeCMS()
	writePath.Put(wal, lsm, key, value)
}

func SecondMenuHLL(hll HLL.HLL, wal *WAL.WAL, lsm *LSM.LSM, key string) {

	reader := bufio.NewReader(os.Stdin)
	for {
		HandlerHLL()
		command := GetInputFromUser("Odaberite operaciju: ", reader)
		if command == "1" {
			element := GetInputFromUser("Dodajte elemente koji želite: ", reader)
			hll.AddData(element)
		} else if command == "2" {
			fmt.Print("Kardinalnost elemenata: ")
			fmt.Println(hll.Estimate())
		} else if command == "3" {
			break
		}
	}
	value := hll.DecodeHLL()
	writePath.Put(wal, lsm, key, value)
}

func GetInputFromUser(prompt string, reader *bufio.Reader) string {
	var input string
	for {
		fmt.Print(prompt)
		input, _ = reader.ReadString('\n')
		input = strings.Replace(input, "\n", "", -1)

		if input == "" {
			fmt.Println("Niste ništa uneli, unesite ponovo!")
		} else {
			break
		}
	}
	return input
}

func GetKeyAndValueFromUser(reader *bufio.Reader) (string, []byte) {
	key := GetInputFromUser("Unesite ključ: ", reader)
	value := []byte(GetInputFromUser("Unesite vrednost: ", reader))
	return key, value
}
