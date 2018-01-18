package goller_test

import (
	"math"
	"testing"
	"time"

	"github.com/rcrowe/goller"
)

func TestDefaultBackoffCalcFunction(t *testing.T) {
	cfg := goller.NewDefaultConfig("", 1)
	minBackoff := func(try int64) int64 {
		pow := math.Pow(float64(try), float64(3))
		return int64(pow + 15 + float64(0*int64(try+1)))
	}
	maxBackoff := func(try int64) int64 {
		pow := math.Pow(float64(try), float64(3))
		return int64(pow + 15 + float64(30*int64(try+1)))
	}

	var backoffs = []struct {
		backoff int64
		min     int64
		max     int64
	}{
		{cfg.Job.BackoffCalc(0), minBackoff(0), maxBackoff(0)},
		{cfg.Job.BackoffCalc(1), minBackoff(1), maxBackoff(1)},
		{cfg.Job.BackoffCalc(2), minBackoff(2), maxBackoff(2)},
		{cfg.Job.BackoffCalc(3), minBackoff(3), maxBackoff(3)},
		{cfg.Job.BackoffCalc(4), minBackoff(4), maxBackoff(4)},
		{cfg.Job.BackoffCalc(5), minBackoff(5), maxBackoff(5)},
		{cfg.Job.BackoffCalc(6), minBackoff(6), maxBackoff(6)},
		{cfg.Job.BackoffCalc(7), minBackoff(7), maxBackoff(7)},
		{cfg.Job.BackoffCalc(8), minBackoff(8), maxBackoff(8)},
		{cfg.Job.BackoffCalc(9), minBackoff(9), maxBackoff(9)},
	}

	for _, b := range backoffs {
		if b.backoff < b.min {
			t.Errorf("backoff `%d` was expected to be greater or equal to `%d`", b.backoff, b.min)
		}

		if b.backoff > b.max {
			t.Errorf("backoff `%d` was expected to be less than or equal to `%d`", b.backoff, b.max)
		}
	}
}

func TestRunOnce(t *testing.T) {
	// Sets flags
	{
		cfg := goller.NewDefaultConfig("", 1)
		cfg.RunOnce()

		if cfg.Consumer.Count != 1 {
			t.Fail()
		}
		if !cfg.Consumer.RunOnce {
			t.Fail()
		}
		if cfg.Consumer.RunSlowly > time.Duration(0) {
			t.Fail()
		}
		if cfg.Consumer.RetrievalMaxNumberOfMessages != 1 {
			t.Fail()
		}
	}

	// Overrides RunSlowly
	{
		cfg := goller.NewDefaultConfig("", 1)
		cfg.RunSlowly(5 * time.Second)
		cfg.RunOnce()

		if cfg.Consumer.Count != 1 {
			t.Fail()
		}
		if !cfg.Consumer.RunOnce {
			t.Fail()
		}
		if cfg.Consumer.RunSlowly > time.Duration(0) {
			t.Fail()
		}
		if cfg.Consumer.RetrievalMaxNumberOfMessages != 1 {
			t.Fail()
		}
	}
}

func TestRunSlowly(t *testing.T) {
	// Sets flags
	{
		cfg := goller.NewDefaultConfig("", 1)
		cfg.RunSlowly(5 * time.Second)

		if cfg.Consumer.Count != 1 {
			t.Fail()
		}
		if cfg.Consumer.RunOnce {
			t.Fail()
		}
		if cfg.Consumer.RunSlowly != 5*time.Second {
			t.Fail()
		}
		if cfg.Consumer.RetrievalMaxNumberOfMessages != 1 {
			t.Fail()
		}
	}

	// Overrides RunSlowly
	{
		cfg := goller.NewDefaultConfig("", 1)
		cfg.RunOnce()
		cfg.RunSlowly(5 * time.Second)

		if cfg.Consumer.Count != 1 {
			t.Fail()
		}
		if cfg.Consumer.RunOnce {
			t.Fail()
		}
		if cfg.Consumer.RunSlowly != 5*time.Second {
			t.Fail()
		}
		if cfg.Consumer.RetrievalMaxNumberOfMessages != 1 {
			t.Fail()
		}
	}
}
