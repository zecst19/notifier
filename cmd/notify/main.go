package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zecst19/notifier"
)

func main() {
	url := flag.String("url", "", "URL to POST notifications to (required)")
	interval := flag.Duration("interval", 5*time.Second, "how often to flush buffered lines")
	workers := flag.Int("workers", 10, "number of concurrent HTTP senders")
	queueSize := flag.Int("queue-size", 512, "max number of queued messages")
	flag.Parse()

	if *url == "" {
		fmt.Println(os.Stderr, "error: --url is required")
		flag.Usage()
		os.Exit(1)
	}

	clint, err := notifier.New(notifier.Config{
		URL:       *url,
		Workers:   *workers,
		QueueSize: *queueSize,
		OnError: func(msg string, err error) {
			log.Printf("notification failed for message %q: %v", msg, err)
		},
	})
	if err != nil {
		log.Fatalf("failed to create notifier: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("stdin read error: %v", err)
		}
	}()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	var batch []string

	flush := func() {
		for _, msg := range batch {
			if err := client.Send(msg); err != nil {
				log.Printf("send error: %v", err)
			}
		}
		batch = batch[:0]
	}

	log.Printf("notifier started: url=%s interval=%s workers=%d", *url, *interval, *workers)

loop:
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				flush()
				break loop
			}
			batch = append(batch, lines)

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			log.Println("shutting down gracefully...")
			for {
				select {
				case line, ok := <-lines:
					if !ok {
						goto done
					}
					batch = append(batch, line)
				default:
					goto done
				}
			}
		done:
			flush()
			break loop
		}
	}

	client.Shutdown()
	log.Println("shutdown complete")
}
