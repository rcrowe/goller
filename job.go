package goller

import (
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
)

// Job lets you interact with the SQS message.
type Job interface {
	// Reader
	Attribute(attr string) (string, bool)
	ID() string
	Body() (string, error)
	Tries() (int64, error)
	Handled() bool

	// Writer
	Delete() error
	Release(secs int64) error
	Backoff() error
}

// ErrAlreadyHandled means the job has already writen back to SQS it's status.
var ErrAlreadyHandled = errors.New("job already handled")

// NewJob creates a new job.
func NewJob(cfg *Config, logger *logrus.Logger, msg sqs.Message, svc sqsiface.SQSAPI) Job {
	return &sqsJob{
		cfg: cfg,
		log: logger,
		msg: msg,
		svc: svc,
	}
}

type sqsJob struct {
	cfg     *Config
	handled bool
	log     *logrus.Logger
	msg     sqs.Message
	svc     sqsiface.SQSAPI
}

// Attribute looks for custom attributes set on the message by the sender.
// If nothing is found it will then look to SQS defined attributes.
func (j *sqsJob) Attribute(attr string) (string, bool) {
	if a, ok := j.msg.MessageAttributes[attr]; ok {
		return *a.StringValue, true
	}

	if a, ok := j.msg.Attributes[attr]; ok {
		return a, true
	}

	return "", false
}

// ID is the unique ID given to it by SQS.
func (j *sqsJob) ID() string {
	return *j.msg.MessageId
}

// Body is the payload of the job.
func (j *sqsJob) Body() (string, error) {
	body := aws.StringValue(j.msg.Body)
	if len(body) == 0 {
		return "", errors.New("job body is empty")
	}

	return body, nil
}

// Tries returns the number of previous attempts to process the job.
// If a job is put back on the queue (un)intentionally this will increase.
func (j *sqsJob) Tries() (int64, error) {
	count, ok := j.Attribute(string(sqs.MessageSystemAttributeNameApproximateReceiveCount))
	if !ok {
		return 0, errors.New("failed to get recieve count off sqs message")
	}

	tries, err := strconv.ParseInt(count, 10, 64)
	if tries > 0 {
		tries--
	}

	return tries, err
}

// Handled returns whether the Goller handler has successfully process the job.
func (j *sqsJob) Handled() bool {
	return j.handled
}

// Delete the message from SQS.
func (j *sqsJob) Delete() error {
	if j.handled {
		return ErrAlreadyHandled
	}

	req := j.svc.DeleteMessageRequest(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(j.cfg.QueueURL),
		ReceiptHandle: j.msg.ReceiptHandle,
	})

	start := time.Now()
	_, err := req.Send()
	sqsJobTimer.Set(time.Since(start).Seconds())

	if err == nil {
		j.handled = true
		j.log.WithField("jid", j.ID()).Debug("job deleted")
	}

	return err
}

// Release the job back on to the SQS queue for the given number of seconds.
func (j *sqsJob) Release(secs int64) error {
	if j.handled {
		return ErrAlreadyHandled
	}

	if secs < j.cfg.Job.MinVisibilityTimeout {
		j.log.WithFields(logrus.Fields{
			"jid":       j.ID(),
			"requested": time.Duration(secs) * time.Second,
		}).Debug("release time is below minimum")

		secs = j.cfg.Job.MinVisibilityTimeout
	}

	if secs > j.cfg.Job.MaxVisibilityTimeout {
		j.log.WithFields(logrus.Fields{
			"jid":       j.ID(),
			"requested": time.Duration(secs) * time.Second,
		}).Debug("release time is above maximum")

		secs = j.cfg.Job.MaxVisibilityTimeout
	}

	// SQS states that the maximum is 43200 seconds, which equals 12 hours.
	// However, it needs to be `< 43200` so enforce that here.
	if secs >= int64((12 * time.Hour).Seconds()) {
		secs--
	}

	req := j.svc.ChangeMessageVisibilityRequest(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(j.cfg.QueueURL),
		ReceiptHandle:     j.msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(secs),
	})

	start := time.Now()
	_, err := req.Send()
	sqsJobTimer.Set(time.Since(start).Seconds())

	if err == nil {
		j.handled = true
		j.log.WithFields(logrus.Fields{
			"jid":  j.ID(),
			"time": time.Duration(secs) * time.Second,
		}).Debug("released job back to SQS")
	}

	return err
}

// Backoff will exponentially release the job back to the queue.
func (j *sqsJob) Backoff() error {
	tries, err := j.Tries()
	if err != nil {
		return err
	}

	j.log.WithFields(logrus.Fields{
		"jid":   j.ID(),
		"tries": tries,
	}).Debug("released job back to SQS")

	backoff := j.cfg.Job.BackoffCalc(tries)

	return j.Release(backoff)
}
