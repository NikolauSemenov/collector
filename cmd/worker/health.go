package main

import (
	"context"
	"errors"
	"net"
	"sync"
)

type TCPHealthListener struct {
	ln        net.Listener
	errCh     chan error
	doneCh    chan struct{}
	closeOnce sync.Once
}

func StartTCPHealthListener(ctx context.Context, addr string) (*TCPHealthListener, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	h := &TCPHealthListener{
		ln:     ln,
		errCh:  make(chan error, 1),
		doneCh: make(chan struct{}),
	}

	go h.run(ctx)

	return h, nil
}

func (h *TCPHealthListener) Addr() net.Addr {
	if h == nil || h.ln == nil {
		return nil
	}
	return h.ln.Addr()
}

func (h *TCPHealthListener) Errors() <-chan error {
	if h == nil {
		ch := make(chan error)
		close(ch)
		return ch
	}
	return h.errCh
}

func (h *TCPHealthListener) Done() <-chan struct{} {
	if h == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return h.doneCh
}

func (h *TCPHealthListener) Close() error {
	if h == nil || h.ln == nil {
		return nil
	}
	var err error
	h.closeOnce.Do(func() {
		err = h.ln.Close()
	})
	return err
}

func (h *TCPHealthListener) run(ctx context.Context) {
	defer close(h.doneCh)
	defer close(h.errCh)

	go func() {
		<-ctx.Done()
		_ = h.Close()
	}()

	for {
		c, err := h.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			select {
			case h.errCh <- err:
			default:
			}
			continue
		}

		_ = c.Close()
	}
}
