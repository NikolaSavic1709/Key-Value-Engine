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
	TokenTime        time.Duration `yaml:"token_time"`
	TokenRequests    int           `yaml:"token_requests"`
}

func (config *Config) ReadConfig() *Config {
	file, err := ioutil.ReadFile("data/configurationFile/configuration.yaml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(file, config)
	if err != nil {
		panic(err)
	}
	return config
}
