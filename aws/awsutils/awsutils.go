package myawsutils

//
//
//   Wrapper functions for the AWS golang api  (& Redis , kind of out of place)
//	 Provides connection methods for S3, SQS (Simple Queue Service),  DDB (Dynamo DB)
//   Assumes Environment Variable AWS_SECRET_ACCESS_KEY, and AWS_ACCESS_KEY_ID are supplied
//	 (Or credentials file is configured)
//
//   Install this module in GOPATH
//

import (
	"fmt"
	"os"

	"github.com/go-redis/redis"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// KeyNotFoundError custom error type, raises KNF on data stores
type KeyNotFoundError struct {
	key     string
	context string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("%s: key %s", e.context, e.key)
}

// Getenv is a wrapper around an environment call
func Getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

// RedisConn returns a redis connection
func RedisConn(db int) (*redis.Client, error) {

	redisdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:6379", Getenv("REDIS_SERVER", "")),
		Password: Getenv("REDIS_PWD", ""),
		DB:       db})

	_, err := redisdb.Ping().Result()

	if err != nil {
		fmt.Println("Error in accessing redis", err.Error())
		return nil, err
	}

	return redisdb, nil
}

// We are using AWS environment variables for credentials

// AwsDDbConn ... returns a DynamoDB connection, & session object
func AwsDDbConn(region string) (*dynamodb.DynamoDB, *session.Session, error) {

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		fmt.Println("DDB session error", err.Error())
		return nil, nil, err
	}

	svc := dynamodb.New(sess)

	return svc, sess, nil
}

// AwsS3Conn returns an S3 connection, and session object
func AwsS3Conn(region string) (*s3.S3, *session.Session, error) {

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		fmt.Println("S3 session error", err.Error())
		return nil, nil, err
	}

	svc := s3.New(sess)

	return svc, sess, nil

}

// AwsSQSConn returns an S3 connection, and session object
func AwsSQSConn(region string) (*sqs.SQS, *session.Session, error) {

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		fmt.Println("SQS session error", err.Error())
		return nil, nil, err
	}

	svc := sqs.New(sess)

	return svc, sess, nil

}
