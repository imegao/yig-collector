package s3client

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"yig-collector/config"
)

type S3Client struct {
	Client *s3.S3
}

func NewS3() *S3Client {
	creds := credentials.NewStaticCredentials(config.Conf.Producer.AccessKey, config.Conf.Producer.SecretKey, "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(config.Conf.Producer.EndPoint),
			Region:      aws.String("r"),
		},
	),
	),
	)
	return &S3Client{s3client}
}