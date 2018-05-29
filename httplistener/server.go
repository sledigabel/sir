package httplistener

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"sync/atomic"
)

// Run is the main loop for HTTP
func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.Addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if h.Certificate != "" {
		cert, err := tls.LoadX509KeyPair(h.Certificate, h.Certificate)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.Listener = l

	if h.Debug {
		log.Printf("Starting listening on %v", h.toString())
	}

	err = http.Serve(h.Listener, h)
	if atomic.LoadInt32(&h.State) != 0 {
		return nil
	}
	return err
}

// Stop is called when the HTTP server is shutdown
func (h *HTTP) Stop() error {
	atomic.StoreInt32(&h.State, 1)
	var err error
	if h.Listener != nil {
		err = h.Listener.Close()
	}
	return err
}
