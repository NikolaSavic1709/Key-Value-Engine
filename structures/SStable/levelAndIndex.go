package SStable

import (
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
)

// FormFilePathsForSSTable forms all file paths for sstable
// based on passed level and index
func FormFilePathsForSSTable(level int, index int) []string {
	levelAndIndexFormat := strconv.Itoa(level) + "_" + strconv.Itoa(index)

	dataFilePath := "data/data/usertable_" + levelAndIndexFormat + "_data.db"
	indexFilePath := "data/index/usertable_" + levelAndIndexFormat + "_index.db"
	summaryFilePath := "data/summary/usertable_" + levelAndIndexFormat + "_summary.db"
	filterFilePath := "data/filter/usertable_" + levelAndIndexFormat + "_filter.db"
	metadataFilePath := "data/metadata/usertable_" + levelAndIndexFormat + "_metadata.db"
	tocFilePath := "data/toc/usertable_" + levelAndIndexFormat + "_toc.txt"

	filePaths := make([]string, 0)
	filePaths = append(filePaths, dataFilePath, indexFilePath, summaryFilePath, filterFilePath, metadataFilePath,
		tocFilePath)

	return filePaths
}

// GetNewIndexForLevel returns new index for passed level
func GetNewIndexForLevel(level int) int {
	return getLastIndexForLevel(level) + 1
}

// GetLevelAndIndexForFileName returns index and level
// for passed value of file name,
func GetLevelAndIndexForFileName(fileName string) (int, int) {
	return getLevelForFileName(fileName), getIndexForFileName(fileName)
}

func getLastIndexForLevel(level int) int {
	files, _ := ioutil.ReadDir("data/data")
	maxIndex := 0

	if len(files) == 0 {
		return maxIndex
	}

	for _, file := range files {
		fileName := file.Name()

		if FileNameMatchesLevel(fileName, level) {
			index := getIndexForFileName(fileName)
			if index > maxIndex {
				maxIndex = index
			}
		}
	}
	return maxIndex
}

func FileNameMatchesLevel(fileName string, level int) bool {
	re := regexp.MustCompile("usertable_" +  strconv.Itoa(level) + "_\\d+")
	match := re.MatchString(fileName)
	if match {
		return true
	}

	return false
}

func getLevelForFileName(fileName string) int {
	fileSplitSlice := strings.Split(fileName, "_")
	level, err := strconv.Atoi(fileSplitSlice[1])
	if err != nil {
		panic(err)
	}

	return level
}

func getIndexForFileName(fileName string) int {
	fileSplitSlice := strings.Split(fileName, "_")
	index, err := strconv.Atoi(fileSplitSlice[2])
	if err != nil {
		panic(err)
	}

	return index
}
