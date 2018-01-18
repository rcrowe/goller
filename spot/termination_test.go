package spot_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rcrowe/goller/spot"
	"github.com/sirupsen/logrus"
)

type stackedWriter struct {
	lock sync.Mutex
	msgs []string
}

func (w *stackedWriter) Write(p []byte) (n int, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.msgs = append(w.msgs, string(p))
	return 0, nil
}

func (w *stackedWriter) Contains(substr string) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	contains := false
	for _, msg := range w.msgs {
		if strings.Contains(msg, substr) {
			contains = true
		}
	}
	return contains
}

func listen(endpoint string) *stackedWriter {
	writer := &stackedWriter{}
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	logger.Out = writer
	ticker := time.NewTicker(10 * time.Second)

	terminator := &spot.TerminationWatcher{
		Endpoint: endpoint,
		Log:      logger,
		Ticker:   ticker,
	}

	terminator.Listen()
	ticker.Stop()
	time.Sleep(50 * time.Millisecond)

	return writer
}

func TestFailedGetLogs(t *testing.T) {
	writer := listen("~~~")

	if !writer.Contains("level=error") {
		t.Fail()
	}
	if !writer.Contains("unsupported protocol scheme") {
		t.Fail()
	}
	if !writer.Contains(spot.ErrTerminationConnection) {
		t.Fail()
	}
}

func TestNotFoundStatusCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	writer := listen(server.URL)

	if !writer.Contains("level=debug") {
		t.Fail()
	}
	if !writer.Contains("code=404") {
		t.Fail()
	}
	if !writer.Contains(spot.InstanceNotTerminating) {
		t.Fail()
	}
}

func TestServerErrorStatusCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	writer := listen(server.URL)

	if !writer.Contains("level=error") {
		t.Fail()
	}
	if !writer.Contains("code=500") {
		t.Fail()
	}
	if !writer.Contains(spot.ErrTerminationStatusCode) {
		t.Fail()
	}
}

func TestIncorrectTerminationTimestamp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))
	defer server.Close()

	writer := listen(server.URL)

	if !writer.Contains("level=error") {
		t.Fail()
	}
	if !writer.Contains(spot.ErrTerminationTimestamp) {
		t.Fail()
	}
}

func TestInstanceTerminating(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("2006-01-02T15:04:05Z"))
	}))
	defer server.Close()

	writer := listen(server.URL)

	if !writer.Contains("level=info") {
		t.Fail()
	}
	if !writer.Contains("when=\"2006-01-02 15:04:05 +0000 UTC\"") {
		t.Fail()
	}
	if !writer.Contains(spot.InstanceTerminating) {
		t.Fail()
	}
}

func TestInstanceTerminatingClosesContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("2006-01-02T15:04:05Z"))
	}))
	defer server.Close()

	ticker := time.NewTicker(10 * time.Second)
	terminator := &spot.TerminationWatcher{
		Endpoint: server.URL,
		Ticker:   ticker,
	}

	ctx, cancel := context.WithCancel(context.Background())
	if ctx.Err() != nil {
		t.Fail()
	}

	terminator.ListenAndCancel(cancel)

	// This would block for ever if the context was not cancelled.
	// We run tests with `-timeout 10s`
	<-ctx.Done()
}
