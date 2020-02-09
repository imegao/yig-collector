package tidbclient

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
	"yig-collector/config"
)

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient() *TidbClient {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", config.Conf.LogPath)
	if err != nil {
		os.Exit(1)
	}
	conn.SetMaxIdleConns(config.Conf.DbMaxIdleConns)
	conn.SetMaxOpenConns(config.Conf.DbMaxOpenConns)
	conn.SetConnMaxLifetime(time.Duration(config.Conf.DbConnMaxLifeSeconds) * time.Second)
	cli.Client = conn
	return cli
}