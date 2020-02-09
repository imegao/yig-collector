package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
	"yig-collector/config"
	"yig-collector/s3client"
	"yig-collector/tidbclient"
)

var logger *log.Logger
var tempBucketName= ""
var counter=0

type ESJsonResponse struct {
	ScrollId string `json:"_scroll_id"`
	Took int `json:"took"`
	Timedout bool `json:"timed_out"`
	Shards  Shard `json:"_shards"`
	Hits    Hit   `json:"hits"`
}

type Shard struct {
	Total int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type Hit struct {
	Total  Total  `json:"total"`
	Maxscore int  `json:"max_score"`
	Hits     []Hits `json:"hits"`
}
type Total struct {
	Value int 	`json:"value"`
	Relation string `json:"relation"`
}

type Hits struct {
	Index string  `json:"_index"`
	Type  string  `json:"_type"`
	Id    string  `json:"_id"`
	Score int     `json:"_score"`
	//Sort  []struct  `json:"sort"`
	Source DataSource `json:"_source"`
}

type DataSource struct {
	Timestamp string `json:"@timestamp"`
	Input InputType `json:"input"`
	BodyBytesSent string `json:"body_bytes_sent"`
	BucketLogging string `json:"bucket_logging"`
	BucketName string `json:"bucket_name"`
	CdnRequest string `json:"cdn_request"`
	ErrorCode string `json:"error_code"`
	HostName string `json:"host_name"`
	HttpReferer string `json:"http_referer"`
	HttpStatus string `json:"http_status"`
	HttpUserAgent string `json:"http_user_agent"`
	HttpXRealIp string `json:"http_x_real_ip"`
	IsInternal string`json:"is_internal"`
	ObjectName string `json:"object_name"`
	ObjectSize string `json:"object_size"`
	Operation string `json:"operation"`
	ProjectId string `json:"project_id"`
	RemoteAddr string `json:"remote_addr"`
	RequestId string `json:"request_id"`
	RequestLength string `json:"request_length"`
	RequestTime string `json:"request_time"`
	RequestUri string `json:"request_uri"`
	RequesterId string `json:"requester_id"`
	ServerCost string `json:"server_cost"`
	StorageClass string `json:"storage_class"`
	TargetStorageClass string `json:"target_storage_class"`
	TimeLocal string `json:"time_local"`
}

type InputType struct {
	Type string `json:"type"`
}


func ParseJsonWithStruct(response io.Reader) (*ESJsonResponse,error){
	data,err:=ioutil.ReadAll(response)
	if err!=nil{
		return nil,err
		fmt.Println("Read file error: ",err.Error())
	}
	configStruct:=&ESJsonResponse{}
	err=json.Unmarshal(data,&configStruct)
	if err!=nil{
		fmt.Println("Json unmarshal error:",err.Error())
		return nil,err
	}

	return configStruct,err
}

func MosaicLog(b DataSource) string {
	logString:= b.RemoteAddr+" ["+b.TimeLocal+"] \""+b.RequestUri+"\" "+b.HttpStatus+" "+b.BodyBytesSent+" "+b.RequestTime+" \""+b.HttpReferer+"\" \""+
		b.HttpUserAgent+"\" \""+b.HostName+"\" \""+b.RequestId+"\" \""+b.BucketLogging+"\" \""+b.RequesterId+"\" \""+b.Operation+"\" \""+
		b.BucketName+" \""+b.ObjectName+"\" "+b.ObjectSize+" "+b.ServerCost+" \""+b.ErrorCode+"\" "+b.RequestLength+" \""+b.ProjectId+"\" \""+
		b.CdnRequest+"\" \""+b.StorageClass+"\" \""+b.TargetStorageClass+"\""
	return logString
}

func HandleRequestAndResponse(url string , postBuffer []byte)(*ESJsonResponse,error){
	//通过时间戳，访问API获取数据  if 返回=null则退出
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(postBuffer))
	if err != nil {
		logger.Println("[ERROR] Http new request error:", err)
	}
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", "Basic ZWxhc3RpYzpSemZ3QDIwMTk=")
	clientScroll := &http.Client{} //创建客户端
	resp, err := clientScroll.Do(request) //发送请求
	defer resp.Body.Close()
	if err != nil {
		logger.Println("[ERROR] Client do error:", err)
	}
	//解析响应的数据
	ResponseData,err:=ParseJsonWithStruct(resp.Body)
	if err != nil {
		logger.Println("[ERROR] Response body read error:", err)

	}

	return ResponseData,err
}

func WriteToLogFile(ResponseData *ESJsonResponse , tc *tidbclient.TidbClient , sc *s3client.S3Client , timestr string) error {
	//// 创建日志文件
	fmt.Println("****************")
	//for 处理数据数组，将数据写入日志文件中
	for i, bucketSource := range ResponseData.Hits.Hits {
		//获取桶名称
		//判断与上一个桶名称是否相同
		bucketName:=bucketSource.Source.BucketName

		fmt.Println(bucketName,  tempBucketName, bucketSource.Source.TimeLocal,"----",i)
		if bucketName == tempBucketName {
			fileInfo,err:=os.Stat(bucketName+timestr+"-"+strconv.Itoa(counter))
			if fileInfo.Size()>=8000{
				//文件满，推上去，不重置桶名字，计数器加1
				//通过临时变量的桶名字访问tidb获取指定桶和指定前缀
				bucket,err:=tc.GetBucket(bucketName)
				if err != nil {
					fmt.Println("Get bucket from tidb failed: ",err.Error())
				}
				//开启桶公共读写
				err = sc.PutBucketAcl(bucketName,"public-read")
				if err != nil {
					fmt.Println("Put bucket ACL failed: ",err.Error())
				}
				//push文件到指定桶中
				f, err := os.OpenFile(bucketName+timestr+"-"+strconv.Itoa(counter), os.O_APPEND, 0666) //打开文件
				TargetPrefix:=bucket.BucketLogging.LoggingEnabled.TargetPrefix
				TargetBucket:=bucket.BucketLogging.LoggingEnabled.TargetBucket
				err = sc.PutObject(TargetBucket, TargetPrefix,f)
				if err != nil {
					fmt.Println("Put object failed: ",err.Error())
				} else {
					err := os.Remove(bucketName+timestr+"-"+strconv.Itoa(counter))
					if err != nil {
						//删除失败
					}
				}
				counter=counter+1
				_, err = os.Create(bucketName+timestr+"-"+strconv.Itoa(counter))
				if err != nil {
					fmt.Println("File open failed: ",err.Error())
				}
			}
			//对json 文件格式化转换
			SingleRowLog:=MosaicLog(bucketSource.Source)
			fmt.Println(SingleRowLog)
			//将该条数据写入文件中
			f, err := os.OpenFile(bucketName+timestr+"-"+strconv.Itoa(counter), os.O_APPEND, 0666) //打开文件
			_, err = io.WriteString(f, SingleRowLog+"\n")
			if err != nil {
				fmt.Println("File write failed: ",err.Error())
			}
		} else {

			//通过临时变量的桶名字访问tidb获取指定桶和指定前缀
			bucket,err:=tc.GetBucket(bucketName)
			if err != nil {
				fmt.Println("Get bucket from tidb failed: ",err.Error())
			}
			//开启桶公共读写
			err = sc.PutBucketAcl(bucketName,"public-read")
			if err != nil {
				fmt.Println("Put bucket ACL failed: ",err.Error())
			}
			//push文件到指定桶中
			f, err := os.OpenFile(bucketName+timestr+"-"+strconv.Itoa(counter), os.O_APPEND, 0666) //打开文件
			TargetPrefix:=bucket.BucketLogging.LoggingEnabled.TargetPrefix
			TargetBucket:=bucket.BucketLogging.LoggingEnabled.TargetBucket
			err = sc.PutObject(TargetBucket, TargetPrefix,f)
			if err != nil {
				fmt.Println("Put object failed: ",err.Error())
			} else {
				err := os.Remove(bucketName+timestr+"-"+strconv.Itoa(counter))
				if err != nil {
					//删除失败
				}
			}
			//更新桶名称临时变量
			tempBucketName = bucketName
			//计数器清零
			counter=0
			//创建日志文件
			f1, err := os.Create(bucketName+timestr+"-"+strconv.Itoa(counter))
			defer f1.Close()
			if err != nil {
				fmt.Println("File open failed: ",err.Error())
			}
			SingleRowLog:=MosaicLog(bucketSource.Source)
			_, err = io.WriteString(f1, SingleRowLog+"\n")
			if err != nil {
				fmt.Println("File write failed: ",err.Error())
			}
			continue
		}
	}
	return nil
}

func runCollector(){
	logger.Println("[INFO] Begin to runCollector", time.Now().Format("2006-01-02 15:04:05"))
	tc:=tidbclient.NewTidbClient()
	sc:=s3client.NewS3()
	// 获取上个小时时间戳
	start,end,timestr:=HourTimestamp()
	start="2020-01-20 03:00:00"
	end="2020-01-20 03:59:59"
    postBuffer :=[]byte(`{"query":{"bool":{"must":[{"range":{"time_local":{"gte":"`+start+`","lt":"`+end+`"}}}]}},"sort":[{"bucket_name.keyword":{"order":"asc"}},{"time_local":{"order":"asc"}}]}`)
	apiIdUrl :="http://10.253.146.68:9200/log_4e2f5e831f4545df852a920f08c9d3c6/_search?scroll=10m"

	ResponseDataContainId,err:=HandleRequestAndResponse(apiIdUrl,postBuffer)
	if err != nil {
		logger.Println("[ERROR] Response body(contain id) read error:", err)
	}
	err = WriteToLogFile(ResponseDataContainId,tc,sc,timestr)
	if err != nil {
		logger.Println("[ERROR] Write to log file is error:", err)
	}
	bufferScroll :=[]byte(`{"scroll":"10m","scroll_id":"`+ResponseDataContainId.ScrollId+`"}`)
	apiScrollUrl :="http://10.253.146.68:9200/_search/scroll"

	for {
		ResponseData,err := HandleRequestAndResponse(apiScrollUrl,bufferScroll)
		if err != nil {
			logger.Println("[ERROR] Response body read error:", err)
		}
		if len(ResponseData.Hits.Hits)==0{
			break
		}
		err = WriteToLogFile(ResponseData,nil,nil,timestr)
		if err != nil {
			logger.Println("[ERROR] Write to log file is error:", err)
		}
  	}

	logger.Println("[INFO] Finish runCollector", time.Now().Format("2006-01-02 15:04:05"))
}

func HourTimestamp() (string,string,string) {
	now := time.Now()
	timestamp := now.Unix() - int64(now.Second()) - int64((60 * now.Minute()))
	endtime:=time.Unix(timestamp-3601,0).Format("2006-01-02 15:04:05")
	starttime:=time.Unix(timestamp-7200,0).Format("2006-01-02 15:04:05")
	timestr:=time.Unix(timestamp-7200,0).Format("2006-01-02-15-04-05")
	return starttime,endtime,timestr
}

func main(){
	//config.ReadConfig()
	//f, err := os.OpenFile(config.Conf.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)//读写，不存在则添加 追加 权限www
	f, err := os.OpenFile("a.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)//读写，不存在则添加 追加 权限www
	if err != nil {
		panic("Failed to open log file " + config.Conf.LogPath)
	}
	defer f.Close()
	logger = log.New(f, "[yig]", log.LstdFlags)  //创建日志格式  日期和时间

	logger.Println("[INFO] Start Yig Collector...")    //日志输出到命令行
	logger.Printf("[TRACE] Config: %+v", config.Conf)   //输出conf信息

	runCollector()
	//c := cron.New()
	//spec:="* * * *"
	//c.AddFunc(spec, runCollector)
	//c.Start()
}