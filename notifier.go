package notifier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type ErrorHandler func(message string, err error)

type Config struct {
	URL         string
	Workers     int
	QueueSize   int
	HTTPTimeout time.Duration
	OnError     ErrorHandler
}

func (c *Config) applyDefatuls() {
	if c.Workers <= 0 {
		c.Workers = 10
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 512
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 10 * time.Second
	}
}

type Client struct {
	cfg   Config
	queue chan string

	stopAccepting chan struct{}
	stopOnce      sync.Once

	reqCtx context.Context

	wg   sync.WaitGroup
	http *http.Client
}

func New(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("notifier: URL must not be empty")
	}
	cfg.applyDefatuls()

	c := &Client{
		cfg:           cfg,
		queue:         make(chan string, cfg.QueueSize),
		stopAccepting: make(chan struct{}),
		reqCtx:        context.Background(),
		http: &http.Client{
			Timeout: cfg.HTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: cfg.Workers,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	for range cfg.Workers {
		c.wg.Add(1)
		go c.worker()
	}

	return c, nil
}

func (c *Client) Send(message string) error {
	select {
	case <-c.stopAccepting:
		return ErrShutdown
	default:
	}

	select {
	case c.queue <- message:
		return nil
	case <-c.stopAccepting:
		return ErrShutdown
	}
}

func (c *Client) Shutdown() {
	c.stopOnce.Do(func() {
		close(c.stopAccepting)
		close(c.queue)
	})
	c.wg.Wait()
}

func (c *Client) worker() {
	defer c.wg.Done()
	for msg := range c.queue {
		c.post(msg)
	}
}

func (c *Client) post(message string) {
	req, err := http.NewRequestWithContext(
		c.reqCtx,
		http.MethodPost,
		c.cfg.URL,
		bytes.NewBufferString(message),
	)
	if err != nil {
		c.handleError(message, fmt.Errorf("building request: %w", err))
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := c.http.Do(req)
	if err != nil {
		c.handleError(message, fmt.Errorf("sending request: %w", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.handleError(message, fmt.Errorf("unexpected status %d", resp.StatusCode))
	}
}

func (c *Client) handleError(message string, err error) {
	if c.cfg.OnError != nil {
		c.cfg.OnError(message, err)
	}
}

var ErrShutdown = fmt.Errorf("notifier: client is shut down")
