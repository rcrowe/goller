package goller

import "github.com/prometheus/client_golang/prometheus"

var (
	// Consumer
	sqsReceiveTimer = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "goller",
		Name:      "sqs_receive_timer",
		Help:      "Time it takes to receive a response back from the SQS receive message request.",
	})

	receiveErrorTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "goller",
		Name:      "receive_error_total",
		Help:      "Counter for number of errors when retrieving messages from SQS.",
	})

	receivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "goller",
		Name:      "received_total",
		Help:      "Counter for number of messages retrieving from SQS.",
	})

	// Job
	sqsJobTimer = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "goller",
		Name:      "sqs_job_timer",
		Help:      "Time it takes to update SQS on a change to messages.",
	})

	jobHandlerTimer = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "goller",
		Name:      "job_handler_timer",
		Help:      "Time it takes for job handler to process.",
	})

	jobProcessedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "goller",
		Name:      "job_processed_total",
		Help:      "Counter for number of jobs successfully processed.",
	})

	jobPanicTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "goller",
		Name:      "job_panic_total",
		Help:      "Counter for number of panics when calling job handler.",
	})

	jobErrorTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "goller",
		Name:      "job_error_total",
		Help:      "Counter for number of errors when calling job handler.",
	})
)

func init() {
	// Consumer
	prometheus.MustRegister(sqsReceiveTimer)
	prometheus.MustRegister(receiveErrorTotal)
	prometheus.MustRegister(receivedTotal)

	// Job
	prometheus.MustRegister(sqsJobTimer)
	prometheus.MustRegister(jobHandlerTimer)
	prometheus.MustRegister(jobProcessedTotal)
	prometheus.MustRegister(jobPanicTotal)
	prometheus.MustRegister(jobErrorTotal)
}
