package goller

import (
	"math"
	"math/rand"
	"time"
)

// NewDefaultConfig sets up sane defaults for Goller config.
func NewDefaultConfig(queueURL string, consumerCount int) *Config {
	return &Config{
		Consumer: &ConsumerConfig{
			Count:                        consumerCount,
			RetrievalErrWait:             30 * time.Second,
			RetrievalMaxNumberOfMessages: 10,
			RetrievalVisibilityTimeout:   int64((10 * time.Minute).Seconds()),
			RetrievalWaitTimeSeconds:     20,
		},
		Job: &JobConfig{
			BackoffCalc: func(try int64) int64 {
				rand.Seed(time.Now().UnixNano())

				// (retry_count ** 3) + 15 + (rand(30) * (retry_count + 1))
				pow := math.Pow(float64(try), float64(3))
				jitter := float64(rand.Int63n(30)*try + 1)

				return int64(pow + 15 + jitter)
			},
			MinVisibilityTimeout: int64((10 * time.Second).Seconds()),
			MaxVisibilityTimeout: int64((12 * time.Hour).Seconds()),
		},
		QueueURL: queueURL,
	}
}

// RunOnce will ensure that Goller only runs once.
// Either one message is returned and process, or if no messages are in thw queue, Goller will exit.
func (cfg *Config) RunOnce() {
	cfg.Consumer.Count = 1
	cfg.Consumer.RunOnce = true
	cfg.Consumer.RunSlowly = time.Duration(0)
	cfg.Consumer.RetrievalMaxNumberOfMessages = 1
}

// RunSlowly will pop 1 message at a time from SQS with a delay in between processing the next one.
// This can be helpful when debugging / monitoring the processing of jobs.
func (cfg *Config) RunSlowly(pause time.Duration) {
	cfg.Consumer.Count = 1
	cfg.Consumer.RunOnce = false
	cfg.Consumer.RunSlowly = pause
	cfg.Consumer.RetrievalMaxNumberOfMessages = 1
}

// Config holds Goller configuration.
// See NewDefaultConfig(...).
type Config struct {
	Consumer *ConsumerConfig
	Job      *JobConfig

	// The URL of the Amazon SQS queue from which messages are received.
	//
	// Queue URLs are case-sensitive.
	QueueURL string
}

// ConsumerConfig holds configuration for the consumer.
type ConsumerConfig struct {
	// Number of workers that listen against the queue.
	Count int

	// If an error occurs trying to retrieve messages,
	// wait this long before attempting a reconnect.
	// Default is 30 seconds.
	RetrievalErrWait time.Duration

	// The maximum number of messages to return.
	// Valid values are 1 to 10. Default is 10.
	RetrievalMaxNumberOfMessages int64

	// The duration (in seconds) that the received messages are hidden from subsequent
	// retrieve requests after being retrieved by a ReceiveMessage request.
	// Default is 10 minutes. Max 12 hours.
	RetrievalVisibilityTimeout int64

	// The duration (in seconds) for which the call waits for a message to arrive
	// in the queue before returning. If a message is available, the call returns
	// sooner than RetrievalWaitTimeSeconds.
	// Must be between 1 and 20 seconds. Default 20 seconds.
	RetrievalWaitTimeSeconds int64

	// Process one job and exit.
	// Zero value disables setting.
	RunOnce bool

	// Runs one receiver and then waits to process the next one
	// using this duration. It super helpful when debugging jobs.
	// Zero value disables the setting.
	RunSlowly time.Duration
}

// JobConfig holds configuration for jobs.
type JobConfig struct {
	// Calculate the time the job should backoff.
	BackoffCalc func(tries int64) int64

	// Minimum visibility timeout allowed.
	// Default 10 seconds.
	MinVisibilityTimeout int64

	// Maximum visibility timeout allowed.
	// Max 12 hours.
	MaxVisibilityTimeout int64
}
