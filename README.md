# yig-collector

## 功能概述

1.每整点(now)从数据中台给出的API获取[(now-2h) ~ (now-1h)]时间段桶日志数据。

2.按桶名称，将桶日志数据保存为单个文件。

3.将桶日志文件推送到用户配置的指定前缀和指定桶中。

## 安装

```
git clone https://github.com/imegao/yig-collector.git
cd yig-collector 
go build
cp yig-collector /usr/bin/
cp config/yig-collector.toml /etc/yig/
cp yig-collector.service /lib/systemd/system/
systemctl start yig-collector.service
```

## 配置

```
log_path = "/var/log/yig/collector.log"   #运行日志路径
bucket_log_path = ""  #暂不使用
tidb_info = "root:@tcp(192.168.2.128:4000)/yig"  #tidb配置
db_max_open_conns = 10240
db_max_idle_conns = 1024
db_conn_max_life_seconds = 300
file_size_limit = 256    #桶日志文件限制，单位：M
api_id_url = "http://10.253.146.68:9200/log_4e2f5e831f4545df852a920f08c9d3c6/_search?scroll=10m"    #指定的查询API
api_scroll_url = "http://10.253.146.68:9200/_search/scroll"  #指定的查询API
[producer]
endpoint = "s3.test.com:8080"   #yig配置
accessKey = "hehehehe"
secretKey = "hehehehe"
```

