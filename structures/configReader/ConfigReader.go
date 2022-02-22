package configReader

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type Config struct {
	SegmentSize       int           `yaml:"segment_size"`
	Lwm               int           `yaml:"lwm"`
	MemtableThreshold float64       `yaml:"memtable_threshold"`
	LsmLevels         int           `yaml:"lsm_levels"`
	LsmLevelMax       int           `yaml:"lsm_level_max"`
	CacheSize         int           `yaml:"cache_size"`
	TokenTime         time.Duration `yaml:"token_time"`
	TokenRequests     int           `yaml:"token_requests"`
}

func (config *Config) ReadConfig() {
	file, err := ioutil.ReadFile("data/configurationFile/configuration.yaml")
	if err != nil || len(file) == 0 {
		config.Lwm = 9
		config.SegmentSize = 5
		config.LsmLevels = 5
		config.LsmLevelMax = 4
		config.MemtableThreshold = 0.8
		config.TokenTime = 10000000000
		config.TokenRequests = 3
		config.CacheSize = 10
	} else {
		err = yaml.Unmarshal(file, config)
		if err != nil {
			panic(err)
		}
	}
}
