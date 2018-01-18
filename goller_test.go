package goller_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/sqsiface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowe/goller"
	"github.com/sirupsen/logrus"
)

type dummyWriter struct {
	lock sync.Mutex
	msg  string
}

func (w *dummyWriter) Write(p []byte) (n int, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.msg = string(p)
	return 0, nil
}

func (w *dummyWriter) ReadMsg() string {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.msg
}

type stackedWriter struct {
	msgs []string
}

func (w *stackedWriter) Write(p []byte) (n int, err error) {
	w.msgs = append(w.msgs, string(p))
	return 0, nil
}

func TestNewSetsQueueURL(t *testing.T) {
	expected := "foobar"
	w := goller.New(sqs.New(aws.Config{}), expected, 1)

	if w.Config().QueueURL != expected {
		t.Errorf("expected queue URL `%s` did not equal `%s`", expected, w.Config().QueueURL)
	}
}

func TestListenLogsRunOnce(t *testing.T) {
	// Logger so we can check what was written
	dummyWriter := &dummyWriter{}
	logger := logrus.New()
	logger.Out = dummyWriter

	// Custom config so we can stop any consumers starting
	cfg := goller.NewDefaultConfig("foobar", 0)
	cfg.RunOnce()
	cfg.Consumer.Count = 0

	listenOnce(sqs.New(aws.Config{}), logger, cfg)

	if !strings.Contains(dummyWriter.msg, "level=debug") {
		t.Errorf("expected `level=debug` in the log but saw `%s`", dummyWriter.msg)
	}
	if !strings.Contains(dummyWriter.msg, "run-once") {
		t.Errorf("expected `run-once` in the log but saw `%s`", dummyWriter.msg)
	}
	if !strings.Contains(dummyWriter.msg, "enabled") {
		t.Errorf("expected `enabled` in the log but saw `%s`", dummyWriter.msg)
	}
}

func TestListenLogsRunSlowly(t *testing.T) {
	// Logger so we can check what was written
	dummyWriter := &dummyWriter{}
	logger := logrus.New()
	logger.Out = dummyWriter

	// Custom config so we can stop any consumers starting
	cfg := goller.NewDefaultConfig("", 0)
	cfg.RunSlowly(5 * time.Second)
	cfg.Consumer.Count = 0

	listenOnce(sqs.New(aws.Config{}), logger, cfg)

	if !strings.Contains(dummyWriter.msg, "level=debug") {
		t.Errorf("expected `level=debug` in the log but saw `%s`", dummyWriter.msg)
	}
	if !strings.Contains(dummyWriter.msg, "run-slowly") {
		t.Errorf("expected `run-once` in the log but saw `%s`", dummyWriter.msg)
	}
	if !strings.Contains(dummyWriter.msg, "enabled") {
		t.Errorf("expected `enabled` in the log but saw `%s`", dummyWriter.msg)
	}
	if !strings.Contains(dummyWriter.msg, "slowly=5s") {
		t.Errorf("expected `slowly=5s` in the log but saw `%s`", dummyWriter.msg)
	}
}

func TestListenCompletesRunSlowly(t *testing.T) {
	// Logger so we can check what was written
	stackedWriter := &stackedWriter{}
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	logger.Out = stackedWriter

	// Custom config so we can stop any consumers starting
	cfg := goller.NewDefaultConfig("", 1)
	cfg.RunSlowly(11 * time.Millisecond)
	cfg.Consumer.RunOnce = true

	w := goller.NewFromConfig(&receiveSQSClient{}, cfg)
	w.WithLogger(logger)
	w.Listen(context.Background(), func(ctx context.Context, j goller.Job) error {
		return nil
	})

	errLog := stackedWriter.msgs[len(stackedWriter.msgs)-2]

	if !strings.Contains(errLog, "level=debug") {
		t.Errorf("expected `level=debug` in the log but saw `%s`", errLog)
	}
	if !strings.Contains(errLog, "`run-slowly` kicking in") {
		t.Errorf("expected \"`run-slowly` kicking in\" in the log but saw `%s`", errLog)
	}
	if !strings.Contains(errLog, "sleep=11ms") {
		t.Errorf("expected `sleep=10ms` in the log but saw `%s`", errLog)
	}
}

func TestListenLogsContextClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Logger so we can check what was written
	dummyWriter := &dummyWriter{}
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	logger.Out = dummyWriter

	// Custom config so we can stop any consumers starting
	cfg := goller.NewDefaultConfig("", 0)

	w := goller.NewFromConfig(sqs.New(aws.Config{}), cfg)
	w.WithLogger(logger)
	w.Listen(ctx, func(ctx context.Context, j goller.Job) error {
		return nil
	})

	cancel()

	// Give enough time to flush the log
	<-time.Tick(10 * time.Millisecond)

	if !strings.Contains(dummyWriter.ReadMsg(), "shutting down") {
		t.Errorf("expected shutdown message but saw `%s`", dummyWriter.msg)
	}
}

type receiveErroredSQSClient struct {
	sqsiface.SQSAPI
	Input  *sqs.ReceiveMessageInput
	Output sqs.ReceiveMessageOutput
	err    error
}

func (c *receiveErroredSQSClient) ReceiveMessageRequest(input *sqs.ReceiveMessageInput) sqs.ReceiveMessageRequest {
	c.Input = input
	if c.err == nil {
		c.err = errors.New("i just errored")
	}
	return sqs.ReceiveMessageRequest{
		Request: &aws.Request{
			Error: c.err,
		},
	}
}

type receiveSQSClient struct {
	sqsiface.SQSAPI
	Input  *sqs.ReceiveMessageInput
	Output sqs.ReceiveMessageOutput
}

func (c *receiveSQSClient) ReceiveMessageRequest(input *sqs.ReceiveMessageInput) sqs.ReceiveMessageRequest {
	c.Input = input
	return sqs.ReceiveMessageRequest{
		Request: &aws.Request{
			Data: &c.Output,
		},
	}
}

func TestReceiveMessageRequestTakesCorrectParams(t *testing.T) {
	svc := &receiveSQSClient{}

	cfg := goller.NewDefaultConfig("https://foo/bar", 1)
	cfg.RunOnce()
	// Customise from defaults
	cfg.Consumer.RetrievalMaxNumberOfMessages = 7
	cfg.QueueURL = "eggs"
	cfg.Consumer.RetrievalVisibilityTimeout = 130
	cfg.Consumer.RetrievalWaitTimeSeconds = 11

	listenOnce(svc, nil, cfg)

	if aws.Int64Value(svc.Input.MaxNumberOfMessages) != 7 {
		t.Errorf("expected MaxNumberOfMessages to be `7` but got `%d`", aws.Int64Value(svc.Input.MaxNumberOfMessages))
	}
	if aws.StringValue(svc.Input.QueueUrl) != "eggs" {
		t.Errorf("expected QueueUrl to be `eggs` but got `%s`", aws.StringValue(svc.Input.QueueUrl))
	}
	if aws.Int64Value(svc.Input.VisibilityTimeout) != 130 {
		t.Errorf("expected VisibilityTimeout to be `130` but got `%d`", aws.Int64Value(svc.Input.VisibilityTimeout))
	}
	if aws.Int64Value(svc.Input.WaitTimeSeconds) != 11 {
		t.Errorf("expected VisibilityTimeout to be `11` but got `%d`", aws.Int64Value(svc.Input.WaitTimeSeconds))
	}
}

func TestReceiveMessageErroringLogsAWSCode(t *testing.T) {
	expected := awserr.New("520", "aws error peeps", nil)

	svc := &receiveErroredSQSClient{err: expected}

	stackedWriter := &stackedWriter{}
	logger := logrus.New()
	logger.Out = stackedWriter

	listenOnce(svc, logger, nil)

	errLog := stackedWriter.msgs[len(stackedWriter.msgs)-2]

	if !strings.Contains(errLog, "level=error") {
		t.Fail()
	}
	if !strings.Contains(errLog, fmt.Sprintf("code=%s", expected.Code())) {
		t.Fail()
	}
	if !strings.Contains(errLog, fmt.Sprintf("error=\"%s\"", expected.Message())) {
		t.Fail()
	}
}

func TestReceiveMessageErroringLogs(t *testing.T) {
	svc := &receiveErroredSQSClient{err: errors.New("one two kayak")}

	stackedWriter := &stackedWriter{}
	logger := logrus.New()
	logger.Out = stackedWriter

	listenOnce(svc, logger, nil)

	errLog := stackedWriter.msgs[len(stackedWriter.msgs)-2]

	if !strings.Contains(errLog, "level=error") {
		t.Fail()
	}
	if !strings.Contains(errLog, "error=\"one two kayak\"") {
		t.Fail()
	}
}

func TestUnhandledJob(t *testing.T) {
	// Build up service to return messages
	svc := &receiveSQSClient{
		Output: sqs.ReceiveMessageOutput{
			Messages: []sqs.Message{
				{
					MessageId: aws.String("abc123"),
					Body:      aws.String("hello from test"),
				},
			},
		},
	}

	listenOnce(svc, nil, nil)

	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fail()
		return
	}

	expectedMetric := "goller_job_error_total"
	var metricValue float64
	for _, metricFamily := range metricFamilies {
		if *metricFamily.Name != expectedMetric {
			continue
		}

		metricValue = metricFamily.Metric[0].Counter.GetValue()
	}

	if metricValue != 1 {
		t.Errorf("expected error metric to be incremented but got `%f`", metricValue)
	}
}

func listenOnce(svc sqsiface.SQSAPI, logger *logrus.Logger, cfg *goller.Config) {
	if logger == nil {
		logger = logrus.New()
		logger.Out = &stackedWriter{}
	}
	logger.Level = logrus.DebugLevel

	if cfg == nil {
		cfg = goller.NewDefaultConfig("foo", 1)
		cfg.RunOnce()
	}

	w := goller.NewFromConfig(svc, cfg)
	w.WithLogger(logger)
	w.Listen(context.Background(), func(ctx context.Context, j goller.Job) error {
		return nil
	})
}
