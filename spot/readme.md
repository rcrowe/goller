## package `spot`

`github.com/rcrowe/goller/spot` lets you monitor for spot termination notifications from AWS.

Why would you do that? So you can safely finish up your Goller handlers before the server goes bye bye.

```golang
terminator := &spot.TerminationWatcher{
    Client: http.Client{Timeout: 2 * time.Second},
}
<-terminator.Listen()
```

`TerminationWatcher` is usually paired with Goller to close a context. That's made easy with the following...

```golang
ctx, cancel := context.WithCancel(context.Background())

// Watcher
terminator := &spot.TerminationWatcher{
    Client: http.Client{Timeout: 2 * time.Second},
}
terminator.ListenAndCancel(cancel)

// Goller
worker.Listen(ctx, func(ctx context.Context, j goller.Job) error {
    ...
})
```
