package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestTCPHealthListener_AcceptsConnections(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := StartTCPHealthListener(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartTCPHealthListener: %v", err)
	}
	defer func() { _ = h.Close() }()

	addr := h.Addr()
	if addr == nil {
		t.Fatalf("expected non-nil addr")
	}

	c, err := net.DialTimeout("tcp", addr.String(), 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = c.Close()
}

func TestTCPHealthListener_CloseStopsListener(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := StartTCPHealthListener(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartTCPHealthListener: %v", err)
	}

	addr := h.Addr().String()

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-h.Done():
	case <-time.After(750 * time.Millisecond):
		t.Fatalf("expected Done() to be closed after Close")
	}

	_, err = net.DialTimeout("tcp", addr, 150*time.Millisecond)
	if err == nil {
		t.Fatalf("expected dial to fail after Close")
	}
}

func TestTCPHealthListener_CancelStopsListener(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	h, err := StartTCPHealthListener(ctx, "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("StartTCPHealthListener: %v", err)
	}

	addr := h.Addr().String()

	cancel()

	select {
	case <-h.Done():
	case <-time.After(750 * time.Millisecond):
		_ = h.Close()
		t.Fatalf("expected Done() to be closed after context cancel")
	}

	_, err = net.DialTimeout("tcp", addr, 150*time.Millisecond)
	if err == nil {
		_ = h.Close()
		t.Fatalf("expected dial to fail after cancel")
	}
}

func TestTCPHealthListener_ConcurrentConnections(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := StartTCPHealthListener(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartTCPHealthListener: %v", err)
	}
	defer func() { _ = h.Close() }()

	addr := h.Addr().String()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)

	dialErrs := make(chan error, n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				dialErrs <- err
				return
			}
			_ = c.Close()
		}()
	}

	wg.Wait()
	close(dialErrs)

	for e := range dialErrs {
		if e != nil && !errors.Is(e, context.Canceled) {
			t.Fatalf("unexpected dial error: %v", e)
		}
	}

	select {
	case e, ok := <-h.Errors():
		if ok && e != nil {
			t.Fatalf("unexpected health listener error: %v", e)
		}
	default:
	}
}

func TestTCPHealthListener_ErrorsChannelCloses(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := StartTCPHealthListener(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartTCPHealthListener: %v", err)
	}

	_ = h.Close()

	select {
	case <-h.Done():
	case <-time.After(750 * time.Millisecond):
		t.Fatalf("expected Done() to close")
	}

	select {
	case _, ok := <-h.Errors():
		if ok {
			for range h.Errors() {
			}
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatalf("expected Errors() to close")
	}
}
