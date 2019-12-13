package main

//
//
// Utility to browse large AWS S3 buckets
// inputs :
//		bucket_name,
//		region,
//		maxpages to scan
//		prefix - bucket prefix folder
//		s3keyOffset - a nesting offset for a bucket key, allows parsing of parent key, and object-ids
// 		verbose - output control
//
//	MySQL parameters :  environment vars - DB_NAME, DB_USER, DB_PASSWORD
//  AWS Credentials : AWS env variables or from the AWS credentials file
//
//  Go modules : Ensure "awsutils" (and "ytapi" where needed) are installed in GOPATH
//
//

import (
	"awsutils"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"os"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type dbError struct {
	err         string
	db_error_id string
}

func (e *dbError) Error() string {
	return fmt.Sprintf("Db %s: %s", e.db_error_id, e.err)
}

type parseError struct {
	err         string
	err_details string
}

func (e *parseError) Error() string {
	return fmt.Sprintf("Parse error %s: %s", e.err_details, e.err)
}

var count = 0
var db *sql.DB
var dbErr error
var sqlInsertStmt string

var s3BucketName, s3Region, verbose, prefix string
var maxPages, s3keyoffset int
var numberOfRetrievedFiles int

func main() {

	// arg : bucket to scan
	flag.StringVar(&s3BucketName, "bucket_name", "scrape-bucket", "input bucket")
	flag.StringVar(&s3Region, "region", "us-east-1", "region")
	// max pages to scan
	flag.IntVar(&maxPages, "maxPages", 10, "max pages 10 default")

	// verbose output
	flag.StringVar(&verbose, "verbose", "n", "verbose y/n")

	// bucket prefix
	flag.StringVar(&prefix, "prefix", "", "bucket prefix")

	// keyoffset
	flag.IntVar(&s3keyoffset, "s3key_offset", 1, "video_id offset in s3_key, using / delimiter")
	flag.Parse()

	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")

	mysqlDb, dbErr := sql.Open("mysql", fmt.Sprintf("%s:%s@/%s?charset=utf8", dbUser, dbPassword, dbName))
	defer mysqlDb.Close()

	if dbErr != nil {
		panic(dbErr)
		os.Exit(-1)
	}

	fmt.Println("testing db-ping")
	dbErr = mysqlDb.Ping()
	if dbErr != nil {
		panic(dbErr)
	} else {
		db = mysqlDb
	}

	fmt.Println("Successfully connected!")

	setInsertStmt()

	s3svc, sess, _ := awsutils.AwsS3Conn(s3Region)
	getBucketObjects(s3svc, sess, s3BucketName)
}

// getBucketObjects loops through keys , pagewise in an S3 bucket
func getBucketObjects(svc *s3.S3, sess *session.Session, bucketName string) {

	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	// Pagination Flag used to check if we need to go further (default is 1000 otherwise)
	truncatedListing := true
	pageCount := 0

	for truncatedListing && pageCount < maxPages {
		resp, err := svc.ListObjectsV2(query)
		pageCount++

		if err != nil {
			fmt.Println(err.Error())
			return
		}
		// Get all objects for this page
		getObjectsPage(resp, svc, bucketName)

		// Set continuation token
		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated

		if verbose == "y" {
			fmt.Printf("page Num %d, recCount %d \n", pageCount, numberOfRetrievedFiles)
		}
	}

}

// getObjectsPage loops through entries in each page
func getObjectsPage(bucketObjectsList *s3.ListObjectsV2Output, s3Client *s3.S3, bucketName string) {

	// Iterate through the files inside the bucket
	for _, key := range bucketObjectsList.Contents {
		s3Key := *key.Key

		count++

		s3Prefix, videoID, parseErr := parseS3Key(s3Key)
		if parseErr != nil {
			continue
		}
		if count%1000 == 0 {
			fmt.Printf("s3key: count %d, key %s, s3prefix %s, videoID %s\n", count, s3Key, s3Prefix, videoID)
		}

		_, dberr := db.Exec(sqlInsertStmt,
			videoID,
			s3BucketName,
			s3Prefix,
			s3Key)
		if dberr != nil {
			fmt.Println("DB insert Error", dberr)
			continue
		}
	}
}

// parseS3Key parses a key value into prefix and videoID depending on how its formatted
func parseS3Key(s3Key string) (string, string, error) {

	if len(strings.TrimSpace(s3Key)) == 0 {
		return "", "", &parseError{"parseS3Key", "S3Key is length 0"}
	}

	words := strings.Split(strings.TrimSpace(s3Key), "/")

	// aa2019/abc.mp4  means offset is 1
	// aa2019/abc/abc.mp4 means offset is 2
	if len(words) < (s3keyoffset + 1) {
		return "", "", &parseError{"parseS3Key", fmt.Sprintf("prefix format error %s ", s3Key)}
	}

	prefix := strings.Join(words[:s3keyoffset], "/")
	videoIDExt := words[s3keyoffset]
	videoIDSplit := strings.Split(videoIDExt, ".")
	if len(videoIDSplit) == 0 {
		return "", "", &parseError{"parseS3Key", fmt.Sprintf("videoID format error %s ", s3Key)}
	}
	videoID := videoIDSplit[0]
	return prefix, videoID, nil

}

// NewNullString database representation for a null value string
func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

// NewNullInt database representation for a null int value
func NewNullInt(s string) sql.NullInt64 {
	if len(s) == 0 {
		return sql.NullInt64{}
	}

	i, _ := strconv.Atoi(s)
	i64 := int64(i)
	return sql.NullInt64{
		Int64: i64,
		Valid: true,
	}
}

// setInsertStmt sets the template of the insert statement
func setInsertStmt() {
	sqlInsertStmt = `
		INSERT INTO scraped_videos (
			video_id,
			bucket_name,
			prefix,
			s3_key
		)
		VALUES (?, ?, ?, ?);
		`
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
