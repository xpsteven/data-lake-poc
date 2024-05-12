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
	s3Bucket       = "athena-20240123"
	s3Path         = "tables/default/products_cdc/"
	awsRegion      = "ap-northeast-1"
	numProducts    = 10000
	outputFilename = "products_data.json.gz"
)

// Product defines the structure for our product data
type Product struct {
	ID         int     `json:"id"`
	Title      string  `json:"title"`
	Code       string  `json:"code"`
	Price      float64 `json:"price"`
	Desc       string  `json:"desc"`
	DlLoadedAt string  `json:"__dlloadedat"`
}

// byteCountSI converts bytes to human readable string format
func byteCountSI(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

func main() {
	gofakeit.Seed(0) // Seed the random number generator

	// Initialize a session using credentials from the shared credentials file ~/.aws/credentials
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	if err != nil {
		log.Fatalf("Failed to create session: %s", err)
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	// Create a buffer to store JSON data and a variable to track uncompressed size
	var jsonData bytes.Buffer
	var uncompressedSize int
	gz := gzip.NewWriter(&jsonData)

	// Generate test data and write it to the gzip writer
	for i := 1; i <= numProducts; i++ {
		product := Product{
			ID:         i,
			Title:      gofakeit.ProductName(),
			Code:       gofakeit.Generate("????-####"),
			Price:      gofakeit.Price(10, 1000),
			Desc:       gofakeit.Paragraph(1, 2, 250, " "),
			DlLoadedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		}

		data, err := json.Marshal(product)
		if err != nil {
			log.Fatalf("Failed to marshal JSON: %s", err)
		}

		uncompressedSize += len(data) // Accumulate uncompressed data size

		if _, err := gz.Write(data); err != nil {
			log.Fatal("Failed to write compressed JSON: ", err)
		}

		if i != numProducts {
			if _, err := gz.Write([]byte("\n")); err != nil { // Add newline between JSON entries
				log.Fatal("Failed to write newline to gzip: ", err)
			}
			uncompressedSize++ // Account for the newline in uncompressed size
		}
	}

	// Close the gzip writer to finalize the compressed data
	if err := gz.Close(); err != nil {
		log.Fatal("Failed to close gzip writer: ", err)
	}

	// Upload the compressed JSON data to S3
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:          aws.String(s3Bucket),
		Key:             aws.String(fmt.Sprintf("%s%s", s3Path, outputFilename)),
		Body:            bytes.NewReader(jsonData.Bytes()),
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		log.Fatalf("Failed to upload data to S3: %s", err)
	}

	// Output file sizes and number of products
	fmt.Printf("Upload completed successfully.\n")
	fmt.Printf("Number of products: %d\n", numProducts)
	fmt.Printf("Uncompressed data size: %s\n", byteCountSI(uncompressedSize))
	fmt.Printf("Compressed data size: %s\n", byteCountSI(jsonData.Len()))
}
