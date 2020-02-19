package main

import (
	"bytes"
	"encoding/json"
	"github.com/imegao/yig-collector/config"
	"github.com/imegao/yig-collector/log"
	"github.com/imegao/yig-collector/s3client"
	_ "github.com/imegao/yig-collector/s3client"
	"github.com/imegao/yig-collector/tidbclient"
	"github.com/robfig/cron"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

var Logger log.Logger
type ESJsonResponse struct {
	ScrollId string `json:"_scroll_id"`
	Took     int    `json:"took"`
	Timedout bool   `json:"timed_out"`
	Shards   Shard  `json:"_shards"`
	Hits     Hit    `json:"hits"`
}
type Shard struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}
type Hit struct {
	Total    Total  `json:"total"`
	Maxscore int    `json:"max_score"`
	Hits     []Hits `json:"hits"`
}
type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}
type Hits struct {
	Index string `json:"_index"`
	Type  string `json:"_type"`
	Id    string `json:"_id"`
	Score int    `json:"_score"`
	Source DataSource `json:"_source"`
}
type DataSource struct {
	Timestamp          string    `json:"@timestamp"`
	Input              InputType `json:"input"`
	BodyBytesSent      string    `json:"body_bytes_sent"`
	BucketLogging      string    `json:"bucket_logging"`
	BucketName         string    `json:"bucket_name"`
	CdnRequest         string    `json:"cdn_request"`
	ErrorCode          string    `json:"error_code"`
	HostName           string    `json:"host_name"`
	HttpReferer        string    `json:"http_referer"`
	HttpStatus         string    `json:"http_status"`
	HttpUserAgent      string    `json:"http_user_agent"`
	HttpXRealIp        string    `json:"http_x_real_ip"`
	IsInternal         string    `json:"is_internal"`
	ObjectName         string    `json:"object_name"`
	ObjectSize         string    `json:"object_size"`
	Operation          string    `json:"operation"`
	ProjectId          string    `json:"project_id"`
	RemoteAddr         string    `json:"remote_addr"`
	RequestId          string    `json:"request_id"`
	RequestLength      string    `json:"request_length"`
	RequestTime        string    `json:"request_time"`
	RequestUri         string    `json:"request_uri"`
	RequesterId        string    `json:"requester_id"`
	ServerCost         string    `json:"server_cost"`
	StorageClass       string    `json:"storage_class"`
	TargetStorageClass string    `json:"target_storage_class"`
	TimeLocal          string    `json:"time_local"`
}
type InputType struct {
	Type string `json:"type"`
}



func ParseJsonWithStruct(response io.Reader) (*ESJsonResponse, error) {
	data, err := ioutil.ReadAll(response)
	if err != nil {
		Logger.Error("Read file error: ", err.Error())
		return nil, err
	}
	configStruct := &ESJsonResponse{}
	err = json.Unmarshal(data, &configStruct)
	if err != nil {
		Logger.Error("Json unmarshal error:", err.Error())
		return nil, err
	}

	return configStruct, err
}

func MosaicLog(b DataSource) string {
	logString := b.RemoteAddr + " [" + b.TimeLocal + "] \"" + b.RequestUri + "\" " + b.HttpStatus + " " + b.BodyBytesSent + " " + b.RequestTime + " \"" + b.HttpReferer + "\" \"" +
		b.HttpUserAgent + "\" \"" + b.HostName + "\" \"" + b.RequestId + "\" \"" + b.BucketLogging + "\" \"" + b.RequesterId + "\" \"" + b.Operation + "\" \"" +
		b.BucketName + " \"" + b.ObjectName + "\" " + b.ObjectSize + " " + b.ServerCost + " \"" + b.ErrorCode + "\" " + b.RequestLength + " \"" + b.ProjectId + "\" \"" +
		b.CdnRequest + "\" \"" + b.StorageClass + "\" \"" + b.TargetStorageClass + "\""
	return logString
}

func HourTimestamp() (string, string, string) {
	now:=time.Now()
	nowHour :=time.Unix(now.Unix()- int64(now.Second()) - int64((60 * now.Minute())), 0)
	h1, _ := time.ParseDuration("-1h1s")
	h2, _ := time.ParseDuration("-2h")
	endTime := nowHour.Add(h1).Format("2006-01-02 15:04:05")
	startTime := nowHour.Add(h2).Format("2006-01-02 15:04:05")
	lastTime := nowHour.Add(h2).Format("2006-01-02-15-04-05")
	return startTime, endTime, lastTime
}

func HandleRequestAndResponse(url string, postBuffer []byte) (*ESJsonResponse, error) {
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(postBuffer))
	if err != nil {
		Logger.Error("Http new request error:", err.Error())
	}
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", "Basic ZWxhc3RpYzpSemZ3QDIwMTk=")
	clientScroll := &http.Client{}
	resp, err := clientScroll.Do(request)
	if err != nil {
		Logger.Error("Client do error:", err.Error())
	}
	defer resp.Body.Close()
	//Parsing the data of the response
	ResponseData, err := ParseJsonWithStruct(resp.Body)
	if err != nil {
		Logger.Error("Response body read error:", err.Error())

	}

	return ResponseData, err
}

func UploadBucketLogFile(bucketName string, tc *tidbclient.TidbClient, sc *s3client.S3Client, timestr string, counter int) {
	filename:=bucketName+timestr+"-"+strconv.Itoa(counter)
	bucket, err := tc.GetBucket(bucketName)
	if err != nil {
		Logger.Error("Get bucket from tidb failed: ", err.Error())
	}
	//TODO:Open bucket public-read
	f, err := os.OpenFile(filename, os.O_APPEND, 6666) //打开文件
	if err != nil {
		Logger.Error("Open file failed: ", err.Error())
	}
	defer f.Close()
	TargetPrefix := bucket.BucketLogging.LoggingEnabled.TargetPrefix
	TargetBucket := bucket.BucketLogging.LoggingEnabled.TargetBucket
	err = sc.PutObject(TargetBucket, TargetPrefix+filename, f)
	if err != nil {
		Logger.Error("Put object failed: ", err.Error())
	}
	err = os.Remove(filename)
	if err != nil {
		Logger.Error("Remove file failed: ", err.Error())
	}

}

func WriteToLogFile(ResponseData *ESJsonResponse, tc *tidbclient.TidbClient, sc *s3client.S3Client, timestr string, counter *int, tempBucketName *string) error {
	//Creat bucket log file
	for _, bucketSource := range ResponseData.Hits.Hits {
		//Judge whether it is the same as the last bucket name
		bucketName := bucketSource.Source.BucketName
		if bucketName == *tempBucketName {
			fileInfo, err := os.Stat(bucketName + timestr + "-" + strconv.Itoa(*counter))
			if err != nil {
				Logger.Error("File write failed: ", err.Error())
			}
			//File full, push up, counter plus 1, create file
			if fileInfo.Size() >= (config.Conf.FileSizeLimit<<20) {
				UploadBucketLogFile(bucketName, tc, sc, timestr,*counter)
				*counter = *counter + 1
				func() {
					f, err := os.Create(bucketName + timestr + "-" + strconv.Itoa(*counter))
					if err != nil {
						Logger.Error("File open failed: ", err.Error())
					}
					defer f.Close()
				}()

			}
			SingleRowLog := MosaicLog(bucketSource.Source)
			func() {
				//Write the data to the file
				f, err := os.OpenFile(bucketName+timestr+"-"+strconv.Itoa(*counter), os.O_APPEND|os.O_WRONLY, 0666)
				_, err = io.WriteString(f, SingleRowLog+"\n")
				if err != nil {
					Logger.Error("File write failed: ", err.Error())
				}
				defer f.Close()
			}()
		} else {
			if *tempBucketName != "" {
				UploadBucketLogFile(*tempBucketName, tc, sc, timestr,*counter)
			}
			//Update bucket name temporary variable
			*tempBucketName = bucketName
			//Counter clear
			*counter = 0
			func() {
				f, err := os.Create(bucketName + timestr + "-" + strconv.Itoa(*counter))
				if err != nil {
					Logger.Error("File open failed: ", err.Error())
				}
				defer f.Close()
				SingleRowLog := MosaicLog(bucketSource.Source)
				_, err = io.WriteString(f, SingleRowLog+"\n")
				if err != nil {
					Logger.Error("File write failed: ", err.Error())
				}
			}()
			continue
		}
	}
	return nil
}

func runCollector() {
	var tempBucketName = ""
	var counter = 0
	Logger.Info("Begin to runCollector", time.Now().Format("2006-01-02 15:04:05"))
	tc, err:= tidbclient.NewTidbClient()
	if err != nil{
		Logger.Error("Response body(contain id) read error:", err.Error())
	}
	sc := s3client.NewS3()
	//generate search start and end time
	start, end, lastTime := HourTimestamp()

	postBuffer := []byte(`{"query":{"bool":{"must":[{"range":{"time_local":{"gte":"` + start + `","lt":"` + end + `"}}}]}},"sort":[{"bucket_name.keyword":{"order":"asc"}},{"time_local":{"order":"asc"}}]}`)
	ResponseDataContainId, err := HandleRequestAndResponse(config.Conf.ApiIdUrl, postBuffer)
	if err != nil {
		Logger.Error("Response body(contain id) read error:", err.Error())
	}
	err = WriteToLogFile(ResponseDataContainId, tc, sc, lastTime,&counter,&tempBucketName)
	if err != nil {
		Logger.Error("Write to log file is error:", err.Error())
	}
	bufferScroll := []byte(`{"scroll":"10m","scroll_id":"` + ResponseDataContainId.ScrollId + `"}`)
    //Through timestamp, access API to get data
	//if return is null, exit
	for {
		ResponseData, err := HandleRequestAndResponse(config.Conf.ApiScrollUrl, bufferScroll)
		if err != nil {
			Logger.Error("Response body read error:", err.Error())
		}
		if len(ResponseData.Hits.Hits) == 0 {
			UploadBucketLogFile(tempBucketName, tc, sc, lastTime,counter)
			break
		}
		err = WriteToLogFile(ResponseData, tc, sc, lastTime,&counter,&tempBucketName)
		if err != nil {
			Logger.Error("Write to log file is error:", err.Error())
		}
	}

	Logger.Info("Finish runCollector", time.Now().Format("2006-01-02 15:04:05"))
}



func main() {
	//Read configure file
	config.ReadConfig()
	Logger = log.NewFileLogger(config.Conf.LogPath, 2)
	defer Logger.Close()
	Logger.Info("Start Yig Collector...")
	Logger.Info("Config: ", config.Conf)
	//Set timer and ever time working
	c := cron.New()
	spec:="20 0 * * *"
	c.AddFunc(spec, runCollector)
	c.Start()
	select {}
}
