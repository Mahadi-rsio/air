package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type ObjectInfo struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag"`
}

type BucketSummary struct {
	Bucket      string       `json:"bucket"`
	Prefix      string       `json:"prefix"`
	ObjectCount int          `json:"object_count"`
	TotalBytes  int64        `json:"total_bytes"`
	Objects     []ObjectInfo `json:"objects"`
}

func envOr(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func main() {
	prefix := flag.String("prefix", "", "Key prefix filter")
	bucket := flag.String("bucket", envOr("AIR_R2_BUCKET", "air-pg-backups"), "R2 bucket name")
	endpoint := flag.String("endpoint", envOr("AIR_R2_ENDPOINT", "ENDPOINT"), "R2 endpoint URL")
	accessKey := flag.String("access-key", envOr("AIR_R2_ACCESS_KEY_ID", "ACCESS_KEY"), "R2 access key")
	secretKey := flag.String("secret-key", envOr("AIR_R2_SECRET_ACCESS_KEY", "SECRET_KEY"), "R2 secret key")
	clean := flag.Bool("clean", false, "Delete matching objects in R2")
	jsonOut := flag.Bool("json", false, "Output JSON")
	flag.Parse()

	if *endpoint == "" || *accessKey == "" || *secretKey == "" || *bucket == "" {
		log.Fatal("missing R2 credentials/endpoint/bucket")
	}

	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
	})

	ctx := context.Background()
	summary := BucketSummary{
		Bucket: *bucket,
		Prefix: *prefix,
	}

	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: prefix,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			log.Fatalf("list objects: %v", err)
		}
		for _, obj := range page.Contents {
			sz := int64(0)
			if obj.Size != nil {
				sz = *obj.Size
			}
			lm := ""
			if obj.LastModified != nil {
				lm = obj.LastModified.UTC().Format("2006-01-02T15:04:05Z")
			}
			etag := ""
			if obj.ETag != nil {
				etag = *obj.ETag
			}
			summary.Objects = append(summary.Objects, ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         sz,
				LastModified: lm,
				ETag:         etag,
			})
			summary.ObjectCount++
			summary.TotalBytes += sz
		}
	}

	if *clean && len(summary.Objects) > 0 {
		for i := 0; i < len(summary.Objects); i += 1000 {
			end := i + 1000
			if end > len(summary.Objects) {
				end = len(summary.Objects)
			}
			var ids []types.ObjectIdentifier
			for _, o := range summary.Objects[i:end] {
				k := o.Key
				ids = append(ids, types.ObjectIdentifier{Key: &k})
			}
			out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: bucket,
				Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)},
			})
			if err != nil {
				log.Fatalf("delete objects: %v", err)
			}
			if len(out.Errors) > 0 {
				log.Fatalf("delete errors: %+v", out.Errors)
			}
		}
		fmt.Fprintf(os.Stderr, "Deleted %d objects (%d bytes) from s3://%s/%s\n", summary.ObjectCount, summary.TotalBytes, *bucket, *prefix)
	}

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(summary)
		return
	}

	fmt.Printf("Bucket: %s | Prefix: %q | Objects: %d | TotalBytes: %d (%.2f MiB)\n",
		summary.Bucket, summary.Prefix, summary.ObjectCount, summary.TotalBytes, float64(summary.TotalBytes)/(1024*1024))
	for _, o := range summary.Objects {
		fmt.Printf("  %10d  %s  %s\n", o.Size, o.LastModified, o.Key)
	}
}
