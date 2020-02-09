package tidbclient

import (
	"encoding/json"
	"fmt"
	"yig-collector/tidbclient/datatype"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	OwnerId    string
	BucketLogging datatype.BucketLoggingStatus
}

func (t *TidbClient) GetBucket(bucketName string) (bucket *Bucket, err error) {
	var bl string
	sqltext := "select bucketname,bl,uid from buckets where bucketname=?;"
	bucket = new(Bucket)
	fmt.Println(sqltext, bucketName)
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&bucket.Name,
		&bl,
		&bucket.OwnerId,
	)
	if err != nil {
		fmt.Println("11111",err.Error())
		return
	}
	err = json.Unmarshal([]byte(bl), &bucket.BucketLogging)
	if err != nil {
		return
	}
	return
}