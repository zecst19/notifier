# notifier

An HTTP notification client library and CLI tool. The library dispatches messages as HTTP POST requests to a configured endpoint, with bounded concurrency and non-blocking sends. The CLI reads lines from stdin and flushes them at a configurable interval.

## Project structure

```
notifier/
├── go.mod
├── notifier.go        # library
├── notifier_test.go
└── cmd/
    └── notify/
        └── main.go    # CLI executable
```

---

## Running the CLI

### Build

```bash
go build -o notify ./cmd/notify
```

### Usage

```
./notify --url=<URL> [--interval=<duration>] [--workers=<n>] [--queue-size=<n>]
```

| Flag | Default | Description |
|---|---|---|
| `--url` | *(required)* | Endpoint to POST notifications to |
| `--interval` | `5s` | How often buffered lines are flushed |
| `--workers` | `10` | Number of concurrent HTTP senders |
| `--queue-size` | `512` | Max messages buffered before Send blocks |

### Example

```bash
# Send lines from a file, one batch every 2 seconds
./notify --url http://localhost:8080/notify --interval 2s < messages.txt

# Pipe live input
tail -f app.log | ./notify --url http://localhost:8080/notify
```

Graceful shutdown on `SIGINT` (Ctrl+C): the process stops reading stdin, flushes any buffered lines, waits for in-flight HTTP requests to complete, then exits.

---

## Using the library

```go
import "github.com/example/notifier"

client, err := notifier.New(notifier.Config{
    URL:     "http://localhost:8080/notify",
    Workers: 10,
    OnError: func(msg string, err error) {
        log.Printf("failed to deliver %q: %v", msg, err)
    },
})
if err != nil {
    log.Fatal(err)
}
defer client.Shutdown()

client.Send("something happened") // returns immediately
```

### Running tests

```bash
go test ./... -race
```

---

## Design decisions

### Worker pool over per-message goroutines

The spec requires handling spikes without exhausting file descriptors. Spawning a goroutine per message would work under low load but fail under a spike - thousands of concurrent goroutines and open TCP connections would quickly hit OS limits. A fixed worker pool with a buffered channel as the job queue gives bounded concurrency, bounded memory, and natural backpressure at configurable limits.

### Two-signal shutdown

Shutdown uses two separate signals rather than one context cancellation:

- `stopAccepting` (a closed channel) - causes `Send` to return `ErrShutdown` immediately, preventing new messages from entering the queue.
- `close(queue)` - signals workers to exit only *after* draining whatever is already in the queue.

A single context cancellation can't do both cleanly: cancelling the context before the queue drains would also cancel in-flight HTTP requests, losing messages. Separating the signals means the queue drains fully before workers exit.

### Request context is not tied to shutdown

HTTP requests use `context.Background()` rather than the shutdown context. This is intentional: if the shutdown context were used, all pending requests would be cancelled the moment `Shutdown()` is called, which would silently drop messages that were already dequeued by a worker. The `http.Client` timeout handles runaway requests instead.

### Caller-owned error handling

The library does not log, retry, or make any policy decisions on failure. It calls the `OnError` handler and moves on. Retries, dead-letter queues, and alerting are concerns for the caller, not the library.

### Blocking Send on full queue

When the queue is full, `Send` blocks rather than dropping the message or returning an error. This is a deliberate choice: silent drops are the worst outcome for a notification system. Callers who want non-blocking behaviour with explicit drop semantics can wrap `Send` in a `select` with a `default` branch themselves.

---

## Assumptions

- Messages are plain UTF-8 strings. The `Content-Type` header is set to `text/plain; charset=utf-8`. If the receiving service expects JSON, the caller should format messages accordingly before passing them to `Send`.
- At-most-once delivery. There is no retry logic. A failed HTTP request (network error, non-2xx status) is reported via `OnError` and not retried.
- The interval in the CLI controls how often buffered lines are flushed, not the rate at which individual HTTP requests are sent. Rate limiting on the HTTP side is handled implicitly by the worker count.