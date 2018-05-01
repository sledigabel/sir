package main

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/sledigabel/sir/httplistener"
)

func main() {

	h := httplistener.NewHTTPWithParameters(":9999", "", "", 180)
	h.DebugConnections = true
	c := make(chan os.Signal, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		log.Printf("Starting server on address: %v\n", h.Addr)
		err := h.Run()
		if err != nil {
			log.Panicf("Error detected while running HTTP server")
		}
		wg.Done()
	}()

	signal.Notify(c, os.Interrupt)
	select {
	case <-c:
		// received ^C
		log.Printf("Caught: INTERRUPT. Stopping...")
		h.Stop()
	}
	wg.Wait()

}
