package spot

import (
	"context"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	metadataTerminationEndpoint = "http://169.254.169.254/latest/meta-data/spot/termination-time"
	terminationTimeFormat       = "2006-01-02T15:04:05Z"
)

// Log messages from the HTTP calls
const (
	InstanceNotTerminating   = "spot instance not marked for termination"
	InstanceTerminating      = "spot instance marked for termination"
	ErrTerminationConnection = "failed to query termination endpoint"
	ErrTerminationStatusCode = "unexpected endpoint response code"
	ErrTerminationTimestamp  = "unable to parse termination timestamp from endpoint"
)

// TerminationWatcher listens and checks whether the spot instance is about to be terminated by AWS.
// This gives you about 2 minutes to shutdown Goller safely.
type TerminationWatcher struct {
	Client   http.Client
	Endpoint string
	Log      *logrus.Logger
	Ticker   *time.Ticker
}

// Listen to the EC2 meta endpoint for termination notice.
// To stop listening call Stop() on TerminationWatcher.Ticker.
func (w *TerminationWatcher) Listen() chan struct{} {
	if w.Endpoint == "" {
		w.Endpoint = metadataTerminationEndpoint
	}
	if w.Log == nil {
		w.Log = logrus.New()
		w.Log.Out = ioutil.Discard
	}
	if w.Ticker == nil {
		w.Ticker = time.NewTicker(5 * time.Second)
	}

	ch := make(chan struct{})

	go func() {
		defer w.Ticker.Stop()
		w.pollEndpoint(ch)
	}()

	return ch
}

func (w *TerminationWatcher) pollEndpoint(ch chan struct{}) {
	logger := w.Log.WithField("endpoint", w.Endpoint)
	logger.Debug("listening for spot instance termination")

	// enter straight away, then wait for ticker
	for ; true; <-w.Ticker.C {
		resp, err := w.Client.Get(w.Endpoint)
		if err != nil {
			logger.WithError(err).Error(ErrTerminationConnection)
			continue
		}
		defer func() {
			if httpBodyErr := resp.Body.Close(); httpBodyErr != nil {
				logger.WithError(httpBodyErr).Error("failed to close HTTP body")
			}
		}()

		// Did we get thumbs up response
		if resp.StatusCode != http.StatusOK {
			statusCodeLogger := logger.WithField("code", resp.StatusCode)
			if resp.StatusCode == http.StatusNotFound {
				statusCodeLogger.Debug(InstanceNotTerminating)
			} else {
				statusCodeLogger.Error(ErrTerminationStatusCode)
			}

			continue
		}

		// Get payload
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.WithError(err).Error("unable to read response from endpoint")
			continue
		}

		// Endpoint response should include timestamp of when instance is terminating
		when, err := time.Parse(terminationTimeFormat, string(body))
		if err != nil {
			logger.WithError(err).Error(ErrTerminationTimestamp)
			continue
		}

		w.Log.WithField("when", when).Info(InstanceTerminating)

		close(ch)
		return
	}
}

// ListenAndCancel will cancel a context when the spot instance is marked for termination.
func (w *TerminationWatcher) ListenAndCancel(cancel context.CancelFunc) {
	go func() {
		<-w.Listen()
		cancel()
	}()
}
