package config

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"log"
)

const configPath = "/etc/yig/yig-collector.toml"
type Config struct {
	LogPath                string        `toml:"log_path"`
	BucketLogPath          string        `toml:"bucket_log_path"`
	TidbInfo               string        `toml:"tidb_info"`
	DbMaxIdleConns         int           `toml:"db_max_open_conns"`
	DbMaxOpenConns         int           `toml:"db_max_idle_conns"`
	DbConnMaxLifeSeconds   int           `toml:"db_conn_max_life_seconds"`
	ApiIdUrl               string        `toml:"api_id_url"`
	ApiScrollUrl           string        `toml:"api_scroll_url"`
	FileSizeLimit          int64         `toml:"file_size_limit"`
	Authorization          string        `toml:"authorization"`
	Producer               DummyProducer `toml:"producer"`
}

type DummyProducer struct {
	EndPoint  string
	AccessKey string
	SecretKey string
}
var Conf Config
func ReadConfig() {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		if err != nil {
			log.Println("Read config error:", err.Error())

		}
	}
	_, err = toml.Decode(string(data), &Conf)
	if err != nil {
		log.Println("Read config error:", err.Error())

	}
}
