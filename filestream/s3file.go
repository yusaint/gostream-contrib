package filestream

import (
	"bufio"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/yusaint/gostream/generic"
)

type S3FileStream struct {
	reader *bufio.Reader
}

type S3FileStreamCfg struct {
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
	Endpoint  string
	Key       string
}

func NewS3FileStream(ctx context.Context, cfg *S3FileStreamCfg) (*S3FileStream, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	session.Must(sess, err)
	output, err := s3.New(sess).GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(cfg.Key),
	})
	if err != nil {
		return nil, err
	}
	return &S3FileStream{reader: bufio.NewReader(output.Body)}, nil
}

func (s *S3FileStream) EstimatedSize() int64 {
	return -1
}

func (s *S3FileStream) ForeachRemaining(sink generic.Consumer) error {
	for {
		isContinue, err := s.TryAdvance(sink)
		if err != nil {
			return err
		} else {
			if !isContinue {
				return nil
			}
		}
	}
}

func (s *S3FileStream) TryAdvance(sink generic.Consumer) (bool, error) {
	if record, err := s.reader.ReadString('\n'); err != nil {
		return false, err
	} else {
		return true, sink.Accept(record)
	}
}
