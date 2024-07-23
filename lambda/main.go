package main

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context, s3Event events.S3Event) (string, error) {
	var wg sync.WaitGroup
	result := make(chan string)
	errChan := make(chan error)

	for _, record := range s3Event.Records {
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key

		wg.Add(1)
		go getObject(ctx, bucket, key, &wg, result, errChan)
	}
	wg.Wait()
	close(result)
	close(errChan)

	select {
	case err := <-errChan:
		return "", err

	case msg := <-result:
		return msg, nil
	}

}

func getObject(ctx context.Context, bucket, key string, wg *sync.WaitGroup, result chan<- string, errChan chan<- error) {
	defer wg.Done()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-north-1"))
	if err != nil {
		errChan <- fmt.Errorf("unable to load sdk config %w", err)
		return
	}

	svc := s3.NewFromConfig(cfg)

	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		errChan <- fmt.Errorf("failed to get object, %w", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		errChan <- fmt.Errorf("failed to read object body, %w", err)
		return
	}

	result <- fmt.Sprintf("Object content: %s", body)

}
