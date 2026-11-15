package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"air-stage3/pkg/cell"
	"air-stage3/pkg/compatibility"
	"air-stage3/pkg/crypto"
	"air-stage3/pkg/scalezero"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Stage3Results struct {
	Timestamp string                    `json:"timestamp"`
	AllPassed bool                      `json:"all_passed"`
	GroupB    scalezero.GroupBReport    `json:"group_b"`
	GroupE    compatibility.GroupEReport `json:"group_e"`
}

func makeR2Client(ctx context.Context) (*s3.Client, string) {
	endpoint := os.Getenv("ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("R2_ENDPOINT")
	}
	accessKey := os.Getenv("ACCESS_KEY")
	if accessKey == "" {
		accessKey = os.Getenv("R2_ACCESS_KEY_ID")
	}
	secretKey := os.Getenv("SECRET_KEY")
	if secretKey == "" {
		secretKey = os.Getenv("R2_SECRET_ACCESS_KEY")
	}
	bucket := os.Getenv("R2_BUCKET")
	if bucket == "" {
		bucket = "air-mvp-test"
	}
	endpoint = strings.Trim(endpoint, "\"' \t")
	accessKey = strings.Trim(accessKey, "\"' \t")
	secretKey = strings.Trim(secretKey, "\"' \t")
	bucket = strings.Trim(bucket, "\"' \t")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		return nil, ""
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, ""
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, bucket
}

func cleanupR2Prefix(ctx context.Context, s3c *s3.Client, bucket, prefix string) {
	if s3c == nil || bucket == "" {
		return
	}
	p := s3.NewListObjectsV2Paginator(s3c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return
		}
		for _, obj := range page.Contents {
			_, _ = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
		}
	}
}

func main() {
	// Ensure PR_SET_CHILD_SUBREAPER so zombie PG17 children can be reaped cleanly
	_, _, _ = syscall.Syscall(syscall.SYS_PRCTL, 36 /* PR_SET_CHILD_SUBREAPER */, 1, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	volDir := "/vol"
	_ = os.RemoveAll(volDir)
	for _, sub := range []string{"pool", "tenants", "staging", "kms", "spool/wal"} {
		if err := os.MkdirAll(filepath.Join(volDir, sub), 0755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s failed: %v\n", sub, err)
			os.Exit(1)
		}
	}
	_ = os.Chown(volDir, 999, 999)

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init KMS failed: %v\n", err)
		os.Exit(1)
	}

	agent, err := cell.NewAgent(volDir, 4, kms)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init Agent failed: %v\n", err)
		os.Exit(1)
	}
	if err := agent.PrewarmPool(4); err != nil {
		fmt.Fprintf(os.Stderr, "prewarm pool failed: %v\n", err)
		os.Exit(1)
	}

	s3c, bucket := makeR2Client(ctx)
	defer cleanupR2Prefix(context.Background(), s3c, bucket, "tenants/tenant_b_")

	groupB, err := scalezero.RunGroupB(ctx, agent, s3c, bucket, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "RunGroupB error: %v\n", err)
		os.Exit(1)
	}

	workDir := filepath.Join(volDir, "gateway_work")
	_ = os.MkdirAll(workDir, 0755)
	groupE, err := compatibility.RunGroupE(ctx, agent, workDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "RunGroupE error: %v\n", err)
		os.Exit(1)
	}

	res := Stage3Results{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		AllPassed: groupB.Pass && groupE.Pass,
		GroupB:    groupB,
		GroupE:    groupE,
	}

	raw, _ := json.MarshalIndent(res, "", "  ")
	outPath := "/app/out/stage3-results.json"
	if _, err := os.Stat("/app/out"); err != nil {
		outPath = "/app/stage3-results.json"
	}
	_ = os.WriteFile(outPath, raw, 0644)

	fmt.Println(strings.Repeat("=", 72))
	fmt.Println(string(raw))
	fmt.Println(strings.Repeat("=", 72))
	if !res.AllPassed {
		os.Exit(1)
	}
}
