package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/brianvoe/gofakeit/v6"
)

const (
	s3Bucket         = "athena-20240123"
	s3Path           = "tables/default/products_cdc/"
	awsRegion        = "ap-northeast-1"
	targetSizeBytes  = 100 * 1024 * 1024  // Target size for each file ~100 MB
	totalSizeBytes   = 1024 * 1024 * 1024 // Total target size ~1 GB
	compressionLevel = gzip.BestCompression
)

// Product defines the structure for our product data
type Product struct {
	ID           int     `json:"id"`
	Title        string  `json:"title"`
	Code         string  `json:"code"`
	Price        float64 `json:"price"`
	Desc         string  `json:"desc"`
	DlCapturedAt string  `json:"__dlcapturedat"`
}

// byteCountBinary converts bytes to human readable string format using binary (base 1024) unit
func byteCountBinary(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func uploadFile(sess *session.Session, jsonData *bytes.Buffer, index int, uncompressedSize, numRows int) {
	uploader := s3manager.NewUploader(sess)
	key := fmt.Sprintf("%sproducts_data_part%d.json.gz", s3Path, index)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:          aws.String(s3Bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(jsonData.Bytes()),
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		log.Fatalf("Failed to upload data to S3: %s", err)
	}
	fmt.Printf("Part %d uploaded successfully. Rows: %d, Uncompressed Size: %s, Compressed Size: %s\n",
		index, numRows, byteCountBinary(uncompressedSize), byteCountBinary(jsonData.Len()))
}

func main() {
	gofakeit.Seed(0)

	// Initialize a session using credentials from the shared credentials file ~/.aws/credentials
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	if err != nil {
		log.Fatalf("Failed to create session: %s", err)
	}

	var currentSize, totalUncompressedSize, totalCompressedSize, totalRows int
	fileIndex := 1

	for currentSize < totalSizeBytes {
		var jsonData bytes.Buffer
		var uncompressedSize, numRows int
		gz, err := gzip.NewWriterLevel(&jsonData, compressionLevel)
		if err != nil {
			log.Fatal("Failed to create gzip writer with level: ", err)
		}

		for jsonData.Len() < targetSizeBytes && currentSize < totalSizeBytes {
			product := Product{
				ID:           gofakeit.Number(1, 100000),
				Title:        gofakeit.ProductName(),
				Code:         gofakeit.Generate("????-####"),
				Price:        gofakeit.Price(10, 1000),
				Desc:         gofakeit.Paragraph(1, 2, 250, " "),
				DlCapturedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
			}

			data, err := json.Marshal(product)
			if err != nil {
				log.Fatalf("Failed to marshal JSON: %s", err)
			}

			if _, err := gz.Write(data); err != nil {
				log.Fatal("Failed to write compressed JSON: ", err)
			}
			if _, err := gz.Write([]byte("\n")); err != nil {
				log.Fatal("Failed to write newline to gzip: ", err)
			}

			numRows++
			uncompressedSize += len(data) + 1 // Plus one for the newline character
			currentSize += len(data) + 1
		}

		if err := gz.Close(); err != nil {
			log.Fatal("Failed to close gzip writer: ", err)
		}

		uploadFile(sess, &jsonData, fileIndex, uncompressedSize, numRows)
		totalUncompressedSize += uncompressedSize
		totalCompressedSize += jsonData.Len()
		totalRows += numRows
		fileIndex++
	}

	fmt.Printf("Total data generated and uploaded: %d parts, %d rows, Total Uncompressed Size: %s, Total Compressed Size: %s\n",
		fileIndex-1, totalRows, byteCountBinary(totalUncompressedSize), byteCountBinary(totalCompressedSize))
}
