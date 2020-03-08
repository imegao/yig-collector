package tidbclient

import (
	"encoding/json"
	"github.com/imegao/yig-collector/tidbclient/datatype"
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
	sqltext := "select bucketname,logging,uid from buckets where bucketname=?;"
	bucket = new(Bucket)

	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&bucket.Name,
		&bl,
		&bucket.OwnerId,
	)
	if err != nil {
		return nil,err
	}
	err = json.Unmarshal([]byte(bl), &bucket.BucketLogging)
	if err != nil {
		return nil,err
	}
	return
}