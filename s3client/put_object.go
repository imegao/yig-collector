package s3client

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"io"
)

func (s3client *S3Client) PutObject(bucketName, key string, value io.Reader) (err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(value),
	}
	if _, err = s3client.Client.PutObject(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) PutBucketAcl(bucketName string, acl string) (err error) {
	params := &s3.PutBucketAclInput{
		Bucket: aws.String(bucketName),
		ACL:    aws.String(acl),
	}
	_, err = s3client.Client.PutBucketAcl(params)
	return err
}