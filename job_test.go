package goller_test

import (
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/sqsiface"
	"github.com/rcrowe/goller"
	"github.com/sirupsen/logrus"
)

type mockSQSClient struct {
	sqsiface.SQSAPI
}

func TestAttribute(t *testing.T) {
	cfg := goller.NewDefaultConfig("", 1)
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// Not set
	{
		attr, ok := goller.NewJob(cfg, logger, sqs.Message{}, &mockSQSClient{}).Attribute("foo")
		if attr != "" {
			t.Errorf("expected empty attribute but got `%s`", attr)
		}
		if ok {
			t.Error("expected `ok=false` but got `ok=true`")
		}
	}

	// Set
	{
		msg := sqs.Message{
			MessageAttributes: map[string]sqs.MessageAttributeValue{
				"foo": sqs.MessageAttributeValue{
					StringValue: aws.String("bar"),
				},
			},
		}

		attr, ok := goller.NewJob(cfg, logger, msg, &mockSQSClient{}).Attribute("foo")
		if attr != "bar" {
			t.Errorf("expected attribute `bar` but got `%s`", attr)
		}
		if !ok {
			t.Error("expected `ok=true` but got `ok=false`")
		}
	}
}

func TestID(t *testing.T) {
	cfg := goller.NewDefaultConfig("", 1)
	expected := "43244-32423-23423423"
	msg := sqs.Message{
		MessageId: aws.String(expected),
	}

	logger := logrus.New()
	logger.Out = ioutil.Discard

	if id := goller.NewJob(cfg, logger, msg, &mockSQSClient{}).ID(); id != expected {
		t.Error("incorrect job id")
	}
}

func TestBody(t *testing.T) {
	cfg := goller.NewDefaultConfig("", 1)
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// Empty body
	{
		body, err := goller.NewJob(cfg, logger, sqs.Message{}, &mockSQSClient{}).Body()
		if body != "" {
			t.Error("expected an empty message body")
		}
		if err == nil {
			t.Error("expected an empty message body")
		}
	}

	// Valid body
	{
		expected := "this is some text"
		msg := sqs.Message{
			Body: aws.String(expected),
		}

		body, err := goller.NewJob(cfg, logger, msg, &mockSQSClient{}).Body()
		if body != expected {
			t.Errorf("expected body to equal `%s` but got `%s`", expected, body)
		}
		if err != nil {
			t.Error("expected no error returned from a valid body")
		}
	}
}

func TestTries(t *testing.T) {
	cfg := goller.NewDefaultConfig("", 1)
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// Unable to get receive count
	{
		tries, err := goller.NewJob(cfg, logger, sqs.Message{}, &mockSQSClient{}).Tries()
		if tries != 0 {
			t.Fail()
		}
		if err == nil {
			t.Fail()
		}
	}

	// Returns 1 less than SQS says
	{
		msg := sqs.Message{
			Attributes: map[string]string{
				string(sqs.MessageSystemAttributeNameApproximateReceiveCount): "3",
			},
		}

		tries, err := goller.NewJob(cfg, logger, msg, &mockSQSClient{}).Tries()
		if err != nil {
			t.Fail()
		}
		if tries != 2 {
			t.Fail()
		}
	}
}

type erroredDeleteSQSClient struct {
	sqsiface.SQSAPI
	Input *sqs.DeleteMessageInput
}

func (c *erroredDeleteSQSClient) DeleteMessageRequest(input *sqs.DeleteMessageInput) sqs.DeleteMessageRequest {
	c.Input = input
	return sqs.DeleteMessageRequest{
		Request: &aws.Request{
			Error: errors.New("i just errored"),
		},
	}
}

type deleteSQSClient struct {
	sqsiface.SQSAPI
	Input  *sqs.DeleteMessageInput
	Output sqs.DeleteMessageOutput
}

func (c *deleteSQSClient) DeleteMessageRequest(input *sqs.DeleteMessageInput) sqs.DeleteMessageRequest {
	c.Input = input
	return sqs.DeleteMessageRequest{
		Request: &aws.Request{
			Data: &c.Output,
		},
	}
}

func TestDelete(t *testing.T) {
	expectedQueueURL := "http://some/queue/url"
	cfg := goller.NewDefaultConfig(expectedQueueURL, 1)
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// On error, is handled still false
	{
		svc := &erroredDeleteSQSClient{}
		j := goller.NewJob(cfg, logger, sqs.Message{}, svc)
		err := j.Delete()

		if err == nil {
			t.Error("delete request should have failed")
		}
		if j.Handled() {
			t.Error("job was not deleted, so should not be marked as handled")
		}
		if aws.StringValue(svc.Input.QueueUrl) != expectedQueueURL {
			t.Errorf(
				"expected `%s` for queue URL but got `%s`",
				expectedQueueURL,
				aws.StringValue(svc.Input.QueueUrl),
			)
		}
	}

	// Successfully delete marks job as handled
	{
		svc := &deleteSQSClient{Output: sqs.DeleteMessageOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)
		err := j.Delete()

		if err != nil {
			t.Errorf("delete request should have succedded but got `%s`", err)
		}
		if !j.Handled() {
			t.Error("job was deleted, so job should be marked as handled")
		}
	}

	// Job can not be deleted twice
	{
		svc := &deleteSQSClient{Output: sqs.DeleteMessageOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)

		err := j.Delete()
		if err != nil {
			t.Errorf("delete request should have succedded but got `%s`", err)
		}

		err = j.Delete()
		if err == nil {
			t.Error("expected calling delete twice to error because already handled")
		}

		if err != goller.ErrAlreadyHandled {
			t.Errorf("expected an instance of ErrAlreadyHandled to be returned, instead `%s` was", err)
		}
	}
}

type erroredReleaseSQSClient struct {
	sqsiface.SQSAPI
	Input *sqs.ChangeMessageVisibilityInput
}

func (c *erroredReleaseSQSClient) ChangeMessageVisibilityRequest(input *sqs.ChangeMessageVisibilityInput) sqs.ChangeMessageVisibilityRequest {
	c.Input = input
	return sqs.ChangeMessageVisibilityRequest{
		Request: &aws.Request{
			Error: errors.New("i just errored"),
		},
	}
}

type releaseSQSClient struct {
	sqsiface.SQSAPI
	Input  *sqs.ChangeMessageVisibilityInput
	Output sqs.ChangeMessageVisibilityOutput
}

func (c *releaseSQSClient) ChangeMessageVisibilityRequest(input *sqs.ChangeMessageVisibilityInput) sqs.ChangeMessageVisibilityRequest {
	c.Input = input
	return sqs.ChangeMessageVisibilityRequest{
		Request: &aws.Request{
			Data: &c.Output,
		},
	}
}

func TestRelease(t *testing.T) {
	expectedQueueURL := "http://some/queue/url"
	cfg := goller.NewDefaultConfig(expectedQueueURL, 1)
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// On error, is handled still false
	{
		svc := &erroredReleaseSQSClient{}
		j := goller.NewJob(cfg, logger, sqs.Message{}, svc)
		err := j.Release(50)

		if err == nil {
			t.Error("release request should have failed")
		}
		if j.Handled() {
			t.Error("job was not released, so should not be marked as handled")
		}
		if aws.StringValue(svc.Input.QueueUrl) != expectedQueueURL {
			t.Errorf(
				"expected `%s` for queue URL but got `%s`",
				expectedQueueURL,
				aws.StringValue(svc.Input.QueueUrl),
			)
		}
	}

	// Successfully delete marks job as handled
	{
		expected := int64(50)

		svc := &releaseSQSClient{Output: sqs.ChangeMessageVisibilityOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)
		err := j.Release(expected)

		if err != nil {
			t.Errorf("release request should have succedded but got `%s`", err)
		}
		if !j.Handled() {
			t.Error("job was released, so job should be marked as handled")
		}
		if aws.Int64Value(svc.Input.VisibilityTimeout) != expected {
			t.Errorf("incorrect visibility timeout. expected `%d` but got `%d`", expected, svc.Input.VisibilityTimeout)
		}
	}
}

func TestReleaseConfigOverrides(t *testing.T) {
	expectedQueueURL := "http://some/queue/url"
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// Min visibility config overrides release request
	{
		expected := int64(100)

		cfg := goller.NewDefaultConfig(expectedQueueURL, 1)
		cfg.Job.MinVisibilityTimeout = expected

		svc := &releaseSQSClient{Output: sqs.ChangeMessageVisibilityOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)
		j.Release(50)

		if aws.Int64Value(svc.Input.VisibilityTimeout) != expected {
			t.Error("incorrect visibility timeout")
		}
	}

	// Max visibility config overrides release request
	{
		expected := int64(40)

		cfg := goller.NewDefaultConfig(expectedQueueURL, 1)
		cfg.Job.MaxVisibilityTimeout = expected

		svc := &releaseSQSClient{Output: sqs.ChangeMessageVisibilityOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)
		j.Release(expected + 10)

		if aws.Int64Value(svc.Input.VisibilityTimeout) != expected {
			t.Error("incorrect visibility timeout")
		}
	}

	// Max visibility can not go over (12hrs - 1second) as per SQS API
	{
		expected := int64(((12 * time.Hour) - 1).Seconds())
		cfg := goller.NewDefaultConfig(expectedQueueURL, 1)

		svc := &releaseSQSClient{Output: sqs.ChangeMessageVisibilityOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)
		j.Release(int64((12 * time.Hour).Seconds()))

		if aws.Int64Value(svc.Input.VisibilityTimeout) != expected {
			t.Errorf("incorrect visibility timeout. expected `%d` but got `%d`", expected, aws.Int64Value(svc.Input.VisibilityTimeout))
		}
	}
}

func TestReleaseCantDeleteTwice(t *testing.T) {
	expectedQueueURL := "http://some/queue/url"
	cfg := goller.NewDefaultConfig(expectedQueueURL, 1)
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// Job can not be deleted twice
	{
		svc := &releaseSQSClient{Output: sqs.ChangeMessageVisibilityOutput{}}
		j := goller.NewJob(cfg, logger, sqs.Message{MessageId: aws.String("123")}, svc)

		err := j.Release(50)
		if err != nil {
			t.Errorf("release request should have succedded but got `%s`", err)
		}

		err = j.Release(50)
		if err == nil {
			t.Error("expected calling release twice to error because already handled")
		}

		if err != goller.ErrAlreadyHandled {
			t.Errorf("expected an instance of ErrAlreadyHandled to be returned, instead `%s` was", err)
		}
	}
}

func TestBackoff(t *testing.T) {
	logger := logrus.New()
	logger.Out = ioutil.Discard

	// Unable to Backoff when unable to get number of tries
	{
		cfg := goller.NewDefaultConfig("", 1)
		err := goller.NewJob(cfg, logger, sqs.Message{}, &mockSQSClient{}).Backoff()
		if err == nil {
			t.Fail()
		}
	}

	// Uses BackoffCalc to get release time
	{
		cfg := goller.NewDefaultConfig("", 1)
		cfg.Job.BackoffCalc = func(try int64) int64 {
			return 12
		}

		msg := sqs.Message{
			MessageId: aws.String("123"),
			Attributes: map[string]string{
				string(sqs.MessageSystemAttributeNameApproximateReceiveCount): "3",
			},
		}

		svc := &releaseSQSClient{Output: sqs.ChangeMessageVisibilityOutput{}}
		j := goller.NewJob(cfg, logger, msg, svc)

		err := j.Backoff()
		if err != nil {
			t.Errorf("expected backoff to succeed. received err `%s`", err)
		}

		if aws.Int64Value(svc.Input.VisibilityTimeout) != 12 {
			t.Error("incorrect visibility timeout")
		}
	}
}
