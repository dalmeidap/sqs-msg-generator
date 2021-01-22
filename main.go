package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

//type Localstack struct {
//	url    string
//	region string
//}
//
//func NewLocalstack(url string, region string) *Localstack {
//	return &Localstack{
//		region: region,
//		url:    region,
//	}
//}
//
//func (l *Localstack) ResolveEndpoint() (aws.Endpoint, error) {
//	return aws.Endpoint{}
//}

var queue = aws.String("trigger_datafridge_events_stg")
var msgPath = "/Volumes/Work/GFS/sqs-msg-generator/events/datafridge_event_example.json"
var messageCount = 200
var workers = 100
var localStackEnabled = false

var awsKey = "DEFINE"
var awsAccessSecret = "DEFINE"
var awsRegion = "DEFINE"

var customResolver = aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
	if localStackEnabled {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:4566",
			SigningRegion: "us-east-1",
		}, nil
	}
	//// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
	return aws.Endpoint{}, &aws.EndpointNotFoundError{}
})

func main() {
	fmt.Println("Version", runtime.Version())
	fmt.Println("NumCPU", runtime.NumCPU())
	fmt.Println("GOMAXPROCS", runtime.GOMAXPROCS(0))

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		config.WithEndpointResolver(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsKey, awsAccessSecret, "")),
		//config.WithSharedConfigProfile("gfs-staging"),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	msgBody, err := ioutil.ReadFile(msgPath)
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}

	sqsClient := sqs.NewFromConfig(cfg)
	queueURL := getQueueUrl(sqsClient)
	//msgInput := buildInput(string(msgBody), queueURL)

	successCount := uint32(0)
	errorCount := uint32(0)
	startTime := time.Now()
	var wg sync.WaitGroup

	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go runWorker(ctx, i, sqsClient, queueURL, string(msgBody), &wg, &successCount, &errorCount)
	}

	wg.Wait()

	fmt.Printf("Sent messages: %v\n", successCount)
	fmt.Printf("Errors: %v\n", errorCount)
	fmt.Printf("Time: %v\n", time.Since(startTime))
}

func runWorker(ctx context.Context, id int, sqsClient *sqs.Client, queueUrl *string, msgBody string, wg *sync.WaitGroup, successCount *uint32, errorCount *uint32) {
	var msgBatchItems []types.SendMessageBatchRequestEntry
	requestNumber := 1

	for i := 1; i <= messageCount; i++ {

		// batchItems only support 10 messages
		wasSent := false
		batchItem := buildBatchInput(msgBody, strconv.Itoa(i))
		msgBatchItems = append(msgBatchItems, batchItem)

		if len(msgBatchItems) == 10 {
			_, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				Entries:  msgBatchItems,
				QueueUrl: queueUrl,
			})

			fmt.Printf("Sending messages [worker:%v] [msgCount: %v] [requestNumber:%v]\n", id, len(msgBatchItems), requestNumber)

			if err != nil {
				fmt.Println("Got an error sending the message:")
				fmt.Printf("Error: %v\n", err)
				atomic.AddUint32(errorCount, 1)

			}
			//fmt.Println("Sent message with ID: " + *resp.MessageId)
			atomic.AddUint32(successCount, 10)
			wasSent = true
			msgBatchItems = nil
			requestNumber++
			continue
		}

		if !wasSent && i == messageCount {

			fmt.Printf("Sending messages [worker:%v] [msgCount: %v] [requestNumber:%v]\n", id, len(msgBatchItems), requestNumber)
			_, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				Entries:  msgBatchItems,
				QueueUrl: queueUrl,
			})
			if err != nil {
				fmt.Println("Got an error sending the message:")
				fmt.Printf("Error: %v\n", err)
				atomic.AddUint32(errorCount, 1)

			}
			//fmt.Println("Sent message with ID: " + *resp.MessageId)
			atomic.AddUint32(successCount, uint32(len(msgBatchItems)))
			wasSent = true
			continue
		}
	}

	wg.Done()
}

func buildBatchInput(msgBody string, id string) types.SendMessageBatchRequestEntry {
	//fmt.Printf("ID mesage: %v\n", id)
	return types.SendMessageBatchRequestEntry{
		Id:           aws.String(id),
		DelaySeconds: 0,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"global_entity_id": {
				DataType:    aws.String("String"),
				StringValue: aws.String("PY_TEST"),
			},
		},
		MessageBody: aws.String(msgBody),
	}
}

//func sendMessages(ctx context.Context, sqsClient sqs.Client, ) {
//	_, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
//		Entries:  msgBatchItems,
//		QueueUrl: queueUrl,
//	})
//
//	fmt.Printf("Sending messages [worker:%v] [msgNum: %v]\n", id, i)
//
//	if err != nil {
//		fmt.Println("Got an error sending the message:")
//		fmt.Printf("Error: %v\n", err)
//		atomic.AddUint32(errorCount, 1)
//		continue
//	}
//}

func getQueueUrl(sqsClient *sqs.Client) *string {
	queueUrlInput := &sqs.GetQueueUrlInput{QueueName: queue}
	result, err := sqsClient.GetQueueUrl(context.Background(), queueUrlInput)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return nil
	}

	return result.QueueUrl
}
