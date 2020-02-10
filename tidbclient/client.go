package tidbclient

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/imegao/yig-collector/config"
	"time"
)

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient() (*TidbClient,error) {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", config.Conf.TidbInfo)
	if err != nil {
		return nil,err
	}
	conn.SetMaxIdleConns(config.Conf.DbMaxIdleConns)
	conn.SetMaxOpenConns(config.Conf.DbMaxOpenConns)
	conn.SetConnMaxLifetime(time.Duration(config.Conf.DbConnMaxLifeSeconds) * time.Second)
	cli.Client = conn
	return cli,nil
}