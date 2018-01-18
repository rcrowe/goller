## package `goller` [![CircleCI](https://circleci.com/gh/rcrowe/goller.svg?style=svg)](https://circleci.com/gh/rcrowe/goller)

Goller is a set of packages to make consuming from SQS silky smooth. You can
get started with...

```golang
svc := sqs.New(cfg)
ctx, cancel := context.WithCancel(context.Background())

worker := goller.New(svc, "https://queue/url", 10)
worker.Listen(ctx, func(ctx context.Context, j goller.Job) error {
    // Your logic goes here
    return j.Delete()
})
```

Everyone has that time when your code just doesn't want to play ball. Goller
gives you `RunOnce` and `RunSlowly` to aid in diagnosing.

```golang
cfg := goller.NewDefaultConfig("https://queue/url", 10)
// Pop one job from SQS & stop Goller
cfg.RunOnce()

worker := goller.NewFromConfig(svc, cfg)
```

Checkout [spot](https://github.com/rcrowe/goller/tree/master/spot) if you want to use Goller on your spot instances.

### logging

By default nothing is logged by Goller - don't you hate those libraries that log :rage: - But depending on your usecase it can be super helpful.

```golang
// Logging uses Logrus
logger := logrus.New()
logger.Level = logrus.DebugLevel

logger = logger.WithFields(logrus.Fields{
    "service": "some-service",
})

worker := goller.New(svc, "https://queue/url", 10)
worker.WithLogger(logger)
```

### prometheus

Goller exports some key metrics to the default Prometheus registery.

```golang
go func() {
    http.Handle("/metrics", promhttp.Handler())
    http.ListenAndServe(":8080", nil)
}()
```
