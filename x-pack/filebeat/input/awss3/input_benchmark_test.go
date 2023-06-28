// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

//go:build integration && aws

package awss3

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/monitoring"
	pubtest "github.com/elastic/beats/v7/libbeat/publisher/testing"
	"github.com/elastic/beats/v7/libbeat/statestore"
	"github.com/elastic/beats/v7/libbeat/statestore/storetest"
	awscommon "github.com/elastic/beats/v7/x-pack/libbeat/common/aws"
)

const (
	cloudtrailTestFile            = "testdata/aws-cloudtrail.json.gz"
	totalListingObjects           = 10000
	totalListingObjectsForInputS3 = totalListingObjects / 5
)

type constantSQS struct {
	msgs []sqs.Message
}

var _ sqsAPI = (*constantSQS)(nil)

func newConstantSQS() *constantSQS {
	return &constantSQS{
		msgs: []sqs.Message{
			newSQSMessage(newS3Event(filepath.Base(cloudtrailTestFile))),
		},
	}
}

func (c *constantSQS) ReceiveMessage(ctx context.Context, maxMessages int) ([]sqs.Message, error) {
	return c.msgs, nil
}

func (*constantSQS) DeleteMessage(ctx context.Context, msg *sqs.Message) error {
	return nil
}

func (*constantSQS) ChangeMessageVisibility(ctx context.Context, msg *sqs.Message, timeout time.Duration) error {
	return nil
}

type s3PagerConstant struct {
	mutex        *sync.Mutex
	objects      []s3.Object
	currentIndex int
}

var _ s3Pager = (*s3PagerConstant)(nil)

func (c *s3PagerConstant) Next(ctx context.Context) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.currentIndex < len(c.objects)
}

func (c *s3PagerConstant) CurrentPage() *s3.ListObjectsOutput {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := &s3.ListObjectsOutput{}
	pageSize := 1000
	if len(c.objects) < c.currentIndex+pageSize {
		pageSize = len(c.objects) - c.currentIndex
	}

	ret.Contents = c.objects[c.currentIndex : c.currentIndex+pageSize]
	c.currentIndex = c.currentIndex + pageSize

	return ret
}

func (c *s3PagerConstant) Err() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.currentIndex >= len(c.objects) {
		c.currentIndex = 0
	}
	return nil
}

func newS3PagerConstant(listPrefix string) *s3PagerConstant {
	lastModified := time.Now()
	ret := &s3PagerConstant{
		mutex:        new(sync.Mutex),
		currentIndex: 0,
	}

	for i := 0; i < totalListingObjectsForInputS3; i++ {
		ret.objects = append(ret.objects, s3.Object{
			Key:          aws.String(fmt.Sprintf("%s-%d.json.gz", listPrefix, i)),
			ETag:         aws.String(fmt.Sprintf("etag-%s-%d", listPrefix, i)),
			LastModified: aws.Time(lastModified),
		})
	}

	return ret
}

type constantS3 struct {
	filename      string
	data          []byte
	contentType   string
	pagerConstant s3Pager
}

var _ s3API = (*constantS3)(nil)

func newConstantS3(t testing.TB) *constantS3 {
	data, err := ioutil.ReadFile(cloudtrailTestFile)
	if err != nil {
		t.Fatal(err)
	}

	return &constantS3{
		filename:    filepath.Base(cloudtrailTestFile),
		data:        data,
		contentType: contentTypeJSON,
	}
}

func (c constantS3) GetObject(ctx context.Context, bucket, key string) (*s3.GetObjectResponse, error) {
	return newS3GetObjectResponse(c.filename, c.data, c.contentType), nil
}

func (c constantS3) ListObjectsPaginator(bucket, prefix string) s3Pager {
	return c.pagerConstant
}

func makeBenchmarkConfig(t testing.TB) config {
	cfg := common.MustNewConfigFrom(`---
queue_url: foo
file_selectors:
-
  regex: '.json.gz$'
  expand_event_list_from_field: Records
`)

	inputConfig := defaultConfig()
	if err := cfg.Unpack(&inputConfig); err != nil {
		t.Fatal(err)
	}
	return inputConfig
}

func makeBenchmarkConfigSQS(t testing.TB, queueURL, region string, maxMessagesInflight int) *common.Config {
	return common.MustNewConfigFrom(fmt.Sprintf(`---
queue_url: %s
default_region: %s,
max_number_of_messages: %d
visibility_timeout: 30s
file_selectors:
-
  regex: '.gz$'
  expand_event_list_from_field: Records
`, queueURL, region, maxMessagesInflight))
}
func benchmarkInputSQS(t *testing.T, maxMessagesInflight int) testing.BenchmarkResult {
	return testing.Benchmark(func(b *testing.B) {
		logp.TestingSetup()
		//metricRegistry := monitoring.NewRegistry()
		//metrics := newInputMetrics(metricRegistry, "test_id")
		// Terraform is used to set up S3 and SQS and must be executed manually.
		tfConfig := getTerraformOutputs(t)

		// Ensure SQS is empty before testing.
		drainSQS(t, tfConfig.AWSRegion, tfConfig.QueueURL)

		uploadS3TestFileMultipleTimes(t, tfConfig.AWSRegion, tfConfig.BucketName, "testdata/aws-cloudtrail.json.gz", 100)

		s3Input := createInput(t, makeBenchmarkConfigSQS(t, tfConfig.QueueURL, tfConfig.AWSRegion, maxMessagesInflight))

		inputCtx, cancel := newV2Context(maxMessagesInflight)
		t.Cleanup(cancel)
		time.AfterFunc(15*time.Second, func() {
			cancel()
		})

		b.ResetTimer()
		start := time.Now()
		client := pubtest.NewChanClient(0)
		defer close(client.Channel)
		go func() {
			for event := range client.Channel {
				// Fake the ACK handling that's not implemented in pubtest.
				event.Private.(*awscommon.EventACKTracker).ACK()
			}
		}()

		var errGroup errgroup.Group
		errGroup.Go(func() error {
			pipeline := pubtest.PublisherWithClient(client)
			return s3Input.Run(inputCtx, pipeline)
		})

		if err := errGroup.Wait(); err != nil {
			t.Fatal(err)
		}
		b.StopTimer()
		elapsed := time.Since(start)

		snap := common.MapStr(monitoring.CollectStructSnapshot(
			monitoring.GetNamespace("dataset").GetRegistry(),
			monitoring.Full,
			false))
		t.Log(snap.StringToPrint())

		baseKey := fmt.Sprintf("%s-%d.", inputID, maxMessagesInflight)
		s3EventsCreatedTotal, _ := snap.GetValue(baseKey + "s3_events_created_total")
		s3BytesProcessedTotal, _ := snap.GetValue(baseKey + "s3_bytes_processed_total")
		sqsMessagesDeletedTotal, _ := snap.GetValue(baseKey + "sqs_messages_deleted_total")

		b.ReportMetric(float64(maxMessagesInflight), "max_messages_inflight")
		b.ReportMetric(elapsed.Seconds(), "sec")

		b.ReportMetric(float64(s3EventsCreatedTotal.(int64)), "events")
		b.ReportMetric(float64(s3EventsCreatedTotal.(int64))/elapsed.Seconds(), "events_per_sec")

		b.ReportMetric(float64(s3BytesProcessedTotal.(int64)), "s3_bytes")
		b.ReportMetric(float64(s3BytesProcessedTotal.(int64))/elapsed.Seconds(), "s3_bytes_per_sec")

		b.ReportMetric(float64(sqsMessagesDeletedTotal.(int64)), "sqs_messages")
		b.ReportMetric(float64(sqsMessagesDeletedTotal.(int64))/elapsed.Seconds(), "sqs_messages_per_sec")

	})
}

func TestBenchmarkInputSQS(t *testing.T) {
	_ = logp.TestingSetup(logp.WithLevel(logp.InfoLevel))

	results := []testing.BenchmarkResult{
		benchmarkInputSQS(t, 1),
		benchmarkInputSQS(t, 2),
		benchmarkInputSQS(t, 4),
		benchmarkInputSQS(t, 8),
		benchmarkInputSQS(t, 16),
		benchmarkInputSQS(t, 32),
		benchmarkInputSQS(t, 64),
		benchmarkInputSQS(t, 128),
		benchmarkInputSQS(t, 256),
		benchmarkInputSQS(t, 512),
		benchmarkInputSQS(t, 1024),
	}

	headers := []string{
		"Max Msgs Inflight",
		"Events per sec",
		"S3 Bytes per sec",
		"Time (sec)",
		"CPUs",
	}
	data := make([][]string, 0)
	for _, r := range results {
		data = append(data, []string{
			fmt.Sprintf("%v", r.Extra["max_messages_inflight"]),
			fmt.Sprintf("%v", r.Extra["events_per_sec"]),
			fmt.Sprintf("%v", humanize.Bytes(uint64(r.Extra["s3_bytes_per_sec"]))),
			fmt.Sprintf("%v", r.Extra["sec"]),
			fmt.Sprintf("%v", runtime.GOMAXPROCS(0)),
		})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.AppendBulk(data)
	table.Render()
}

func benchmarkInputS3(t *testing.T, numberOfWorkers int) testing.BenchmarkResult {
	return testing.Benchmark(func(b *testing.B) {
		log := logp.NewLogger(inputName)
		log.Infof("benchmark with %d number of workers", numberOfWorkers)
		metricRegistry := monitoring.NewRegistry()
		metrics := newInputMetrics(metricRegistry, "test_id")

		client := pubtest.NewChanClientWithCallback(100, func(event beat.Event) {
			event.Private.(*awscommon.EventACKTracker).ACK()
		})

		defer func() {
			_ = client.Close()
		}()

		config := makeBenchmarkConfig(t)

		b.ResetTimer()
		start := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		b.Cleanup(cancel)

		go func() {
			for metrics.s3ObjectsAckedTotal.Get() < totalListingObjects {
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
		}()

		errChan := make(chan error)
		wg := new(sync.WaitGroup)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) {
				defer wg.Done()
				listPrefix := fmt.Sprintf("list_prefix_%d", i)
				s3API := newConstantS3(t)
				s3API.pagerConstant = newS3PagerConstant(listPrefix)

				storeReg := statestore.NewRegistry(storetest.NewMemoryStoreBackend())
				store, err := storeReg.Get("test")
				if err != nil {
					errChan <- fmt.Errorf("Failed to access store: %w", err)
					return
				}

				err = store.Set(awsS3WriteCommitPrefix+"bucket"+listPrefix, &commitWriteState{time.Time{}})
				if err != nil {
					errChan <- err
					return
				}

				s3EventHandlerFactory := newS3ObjectProcessorFactory(log.Named("s3"), metrics, s3API, client, config.FileSelectors)
				s3Poller := newS3Poller(logp.NewLogger(inputName), metrics, s3API, s3EventHandlerFactory, newStates(inputCtx), store, "bucket", listPrefix, "region", numberOfWorkers, time.Second)

				if err := s3Poller.Poll(ctx); err != nil {
					if !errors.Is(err, context.DeadlineExceeded) {
						errChan <- err
					}
				}
			}(i, wg)
		}

		wg.Wait()
		select {
		case err := <-errChan:
			if err != nil {
				t.Fatal(err)
			}
		default:

		}

		b.StopTimer()
		elapsed := time.Since(start)

		b.ReportMetric(float64(numberOfWorkers), "number_of_workers")
		b.ReportMetric(elapsed.Seconds(), "sec")

		b.ReportMetric(float64(metrics.s3EventsCreatedTotal.Get()), "events")
		b.ReportMetric(float64(metrics.s3EventsCreatedTotal.Get())/elapsed.Seconds(), "events_per_sec")

		b.ReportMetric(float64(metrics.s3BytesProcessedTotal.Get()), "s3_bytes")
		b.ReportMetric(float64(metrics.s3BytesProcessedTotal.Get())/elapsed.Seconds(), "s3_bytes_per_sec")

		b.ReportMetric(float64(metrics.s3ObjectsListedTotal.Get()), "objects_listed")
		b.ReportMetric(float64(metrics.s3ObjectsListedTotal.Get())/elapsed.Seconds(), "objects_listed_per_sec")

		b.ReportMetric(float64(metrics.s3ObjectsProcessedTotal.Get()), "objects_processed")
		b.ReportMetric(float64(metrics.s3ObjectsProcessedTotal.Get())/elapsed.Seconds(), "objects_processed_per_sec")

		b.ReportMetric(float64(metrics.s3ObjectsAckedTotal.Get()), "objects_acked")
		b.ReportMetric(float64(metrics.s3ObjectsAckedTotal.Get())/elapsed.Seconds(), "objects_acked_per_sec")

	})
}

func TestBenchmarkInputS3(t *testing.T) {
	_ = logp.TestingSetup(logp.WithLevel(logp.InfoLevel))

	results := []testing.BenchmarkResult{
		benchmarkInputS3(t, 1),
		benchmarkInputS3(t, 2),
		benchmarkInputS3(t, 4),
		benchmarkInputS3(t, 8),
		benchmarkInputS3(t, 16),
		benchmarkInputS3(t, 32),
		benchmarkInputS3(t, 64),
		benchmarkInputS3(t, 128),
		benchmarkInputS3(t, 256),
		benchmarkInputS3(t, 512),
		benchmarkInputS3(t, 1024),
	}

	headers := []string{
		"Number of workers",
		"Objects listed total",
		"Objects listed per sec",
		"Objects processed total",
		"Objects processed per sec",
		"Objects acked total",
		"Objects acked per sec",
		"Events total",
		"Events per sec",
		"S3 Bytes total",
		"S3 Bytes per sec",
		"Time (sec)",
		"CPUs",
	}
	data := make([][]string, 0)
	for _, r := range results {
		data = append(data, []string{
			fmt.Sprintf("%v", r.Extra["number_of_workers"]),
			fmt.Sprintf("%v", r.Extra["objects_listed"]),
			fmt.Sprintf("%v", r.Extra["objects_listed_per_sec"]),
			fmt.Sprintf("%v", r.Extra["objects_processed"]),
			fmt.Sprintf("%v", r.Extra["objects_processed_per_sec"]),
			fmt.Sprintf("%v", r.Extra["objects_acked"]),
			fmt.Sprintf("%v", r.Extra["objects_acked_per_sec"]),
			fmt.Sprintf("%v", r.Extra["events"]),
			fmt.Sprintf("%v", r.Extra["events_per_sec"]),
			fmt.Sprintf("%v", humanize.Bytes(uint64(r.Extra["s3_bytes"]))),
			fmt.Sprintf("%v", humanize.Bytes(uint64(r.Extra["s3_bytes_per_sec"]))),
			fmt.Sprintf("%v", r.Extra["sec"]),
			fmt.Sprintf("%v", runtime.GOMAXPROCS(0)),
		})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.AppendBulk(data)
	table.Render()
}
