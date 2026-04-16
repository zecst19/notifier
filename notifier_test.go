package notifier_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zecst19/notifier"
)

func NewTestServer(t *testing.T, statusCode int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = body
		count.Add(1)
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(srv.Close)
	return srv, &count
}

func TestSendDeliverMessages(t *testing.T) {
	srv, count := NewTestServer(t, http.StatusOK)

	c, err := notifier.New(notifier.Config{URL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const n = 50
	for i := range n {
		if err := c.Send(fmt.Sprintf("msg-%d", i)); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}

	c.Shutdown()

	if got := count.Load(); got != n {
		t.Errorf("server received %d messages, want %d", got, n)
	}
}

func TestErrorHandlerCalledOnBadStatus(t *testing.T) {
	srv, _ := NewTestServer(t, http.StatusInternalServerError)

	var errCount atomic.Int64
	c, _ := notifier.New(notifier.Config{
		URL:     srv.URL,
		Workers: 2,
		OnError: func(_ string, _ error) {
			errCount.Add(1)
		},
	})

	const n = 5
	for i := range n {
		_ = c.Send(fmt.Sprintf("msg-%d", i))
	}
	c.Shutdown()

	if got := errCount.Load(); got != n {
		t.Errorf("OnError called %d times, want %d", got, n)
	}
}

func TestSendAfterShutdownReturnsError(t *testing.T) {
	srv, _ := NewTestServer(t, http.StatusOK)
	c, _ := notifier.New(notifier.Config{URL: srv.URL})
	c.Shutdown()

	if err := c.Send("late"); err != notifier.ErrShutdown {
		t.Errorf("got %v, want ErrShutdown", err)
	}
}

func TestShutdownDrainsQueue(t *testing.T) {
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, _ := notifier.New(notifier.Config{URL: srv.URL, Workers: 4, QueueSize: 20})

	const n = 10
	for i := range n {
		_ = c.Send(fmt.Sprintf("msg-%d", i))
	}
	c.Shutdown()

	if got := count.Load(); got != n {
		t.Errorf("after Shutdown, server received %d messages, want %d", got, n)
	}
}

func TestNewRejectsEmptyURL(t *testing.T) {
	_, err := notifier.New(notifier.Config{})
	if err == nil {
		t.Error("expected error for empty URL, got nil")
	}
}
