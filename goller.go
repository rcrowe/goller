package goller

import (
	"context"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
)

const (
	// VERSION is the tagged client version
	VERSION = "0.1.0-dev"
)

// Worker is the core interface for Goller allowing you to start listening to SQS.
// Out of the box most sane defaults are set for you, the only hard requirement is
// you set the queue URL and consumer count; there are two options for setting the URL,
// either goller.New(svc, "https://queue/url/here", 10) or goller.NewFromConfig(svc, cfg).
type Worker interface {
	Config() Config
	WithLogger(logger *logrus.Logger)
	Listen(ctx context.Context, handler HandlerFunc)
}

// HandlerFunc receives any job popped off the SQS queue.
type HandlerFunc func(ctx context.Context, j Job) error

// New is the recommended way of setting up a new instance of Goller with sane defaults.
func New(svc sqsiface.SQSAPI, queueURL string, consumerCount int) Worker {
	cfg := NewDefaultConfig(queueURL, consumerCount)
	return NewFromConfig(svc, cfg)
}

// NewFromConfig is for those power users that know what they want to change in the config.
func NewFromConfig(svc sqsiface.SQSAPI, cfg *Config) Worker {
	logger := logrus.New()
	logger.Out = ioutil.Discard

	return &sqsWorker{
		cfg: cfg,
		log: logger,
		svc: svc,
	}
}

type sqsWorker struct {
	cfg *Config
	log *logrus.Logger
	svc sqsiface.SQSAPI
}

// Config gives you read-only access to how Goller was configured.
func (w *sqsWorker) Config() Config {
	return *w.cfg
}

// WithLogger overrides the default logger.
// By default no logs are ever writen, so if you want output you're going
// to need to set your own logger.
func (w *sqsWorker) WithLogger(logger *logrus.Logger) {
	w.log = logger
}

// Listen to new SQS jobs.
// Context allows you to gracefully shutdown the listener.
func (w *sqsWorker) Listen(ctx context.Context, handler HandlerFunc) {
	// Welcome banner
	w.log.WithFields(logrus.Fields{
		"version":   VERSION,
		"consumers": w.cfg.Consumer.Count,
	}).Info("Starting Goller")
	if w.cfg.Consumer.RunOnce {
		w.log.Debug("`run-once` enabled")
	}
	if w.cfg.Consumer.RunSlowly > time.Duration(0) {
		w.log.WithField("slowly", w.cfg.Consumer.RunSlowly.String()).Debug("`run-slowly` enabled")
	}

	go func() {
		<-ctx.Done()
		if ctx.Err() != nil {
			w.log.Info("shutting down safely...this could take a while")
		}
	}()

	// Start those consumers up
	var wg sync.WaitGroup
	wg.Add(w.cfg.Consumer.Count)

	for i := 0; i < w.cfg.Consumer.Count; i++ {
		go func() {
			defer wg.Done()

			w.receive(ctx, handler)
		}()
	}

	wg.Wait()
}

func (w *sqsWorker) receive(ctx context.Context, handler HandlerFunc) {
	for {
		select {
		case <-ctx.Done():
			w.log.Debug("context done. stopping consume loop.")
			return

		default:
			w.log.WithFields(logrus.Fields{
				"wait": time.Duration(w.cfg.Consumer.RetrievalWaitTimeSeconds) * time.Second,
			}).Debug("calling receive.")

			// Poll SQS for new messages
			// Currently not setting req.SetContext() as it can leave messages dangling with default visibility timeout
			req := w.svc.ReceiveMessageRequest(&sqs.ReceiveMessageInput{
				AttributeNames: []sqs.QueueAttributeName{
					sqs.QueueAttributeName(sqs.MessageSystemAttributeNameApproximateReceiveCount), // j.Tries()
				},
				MaxNumberOfMessages: aws.Int64(w.cfg.Consumer.RetrievalMaxNumberOfMessages),
				MessageAttributeNames: []string{
					"All",
				},
				QueueUrl:          aws.String(w.cfg.QueueURL),
				VisibilityTimeout: aws.Int64(w.cfg.Consumer.RetrievalVisibilityTimeout),
				WaitTimeSeconds:   aws.Int64(w.cfg.Consumer.RetrievalWaitTimeSeconds),
			})

			start := time.Now()
			resp, err := req.Send()
			sqsReceiveTimer.Set(time.Since(start).Seconds())

			if err != nil {
				receiveErrorTotal.Inc()

				if awsErr, ok := err.(awserr.Error); ok {
					w.log.WithFields(logrus.Fields{
						"code":  awsErr.Code(),
						"error": awsErr.Message(),
					}).Error("error receiving sqs message")
				} else {
					w.log.WithError(err).Error("error receiving sqs message")
				}

				if w.cfg.Consumer.RunOnce {
					w.log.Debug("`run-once` complete")
					return
				}

				// Backoff trying to re-connect
				w.log.WithField("sleep", w.cfg.Consumer.RetrievalErrWait).Debug("sleeping before retrying")
				time.Sleep(w.cfg.Consumer.RetrievalErrWait)

				continue
			}

			// Handle response
			receivedTotal.Add(float64(len(resp.Messages)))

			if len(resp.Messages) == 0 {
				w.log.Debug("no messages on attempt. trying again.")
			} else {
				w.log.WithField("count", len(resp.Messages)).Debug("messages retrieved")
				// Pass messages to job handler
				w.handleResponse(ctx, resp.Messages, handler)
			}

			if w.cfg.Consumer.RunSlowly > time.Duration(0) {
				w.log.WithField("sleep", w.cfg.Consumer.RunSlowly).Debug("`run-slowly` kicking in")
				time.Sleep(w.cfg.Consumer.RunSlowly)
			}

			if w.cfg.Consumer.RunOnce {
				w.log.Debug("`run-once` complete")
				return
			}
		}
	}
}

func (w *sqsWorker) handleResponse(ctx context.Context, msgs []sqs.Message, handler HandlerFunc) {
	// Call the handler for each of the messages
	var wg sync.WaitGroup
	wg.Add(len(msgs))

	for _, msg := range msgs {
		j := NewJob(w.cfg, w.log, msg, w.svc)

		logger := w.log.WithField("jid", j.ID())
		logger.Debug("processing job")

		go func(j Job) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("panic: %s", r)
					logger.WithError(err).Error("job handler paniced")
					jobPanicTotal.Inc()
					jobErrorTotal.Inc()
				}
			}()

			start := time.Now()
			err := handler(ctx, j)
			jobHandlerTimer.Set(time.Since(start).Seconds())

			if err != nil {
				logger.WithError(err).Error("handler errored")
				jobErrorTotal.Inc()
			}

			if !j.Handled() {
				logger.WithError(err).Error("job not handled")
				jobErrorTotal.Inc()
			} else {
				logger.Debug("job processed successfully")
				jobProcessedTotal.Inc()
			}
		}(j)
	}

	wg.Wait()
}
