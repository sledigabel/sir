package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/sledigabel/sir/relay"
)

var defaultConf = `
debug = true

[listener]
	addr = ":9090"
	timeout = 60
	debug = true
	log = true

[internal]
enable = true
frequency = "5s"

[backend]

	[server.1]
	alias = "local"
	server_name = "localhost"
	port = 8086
	secure = false
	debug = true
	timeout = "40s"

`

func main() {

	var config = flag.String("file", "sir.conf", "Configuration file for SIR")
	//var debug = flag.Bool("debug", false, "Debug Mode")

	var confstring string

	c := make(chan os.Signal, 1)

	// read the config file or just go with defaults.
	fd, err := os.Open(*config)
	if err != nil {
		log.Printf("Unable to open %v, using default config", *config)
		confstring = defaultConf
	} else {
		b, err := ioutil.ReadAll(fd)
		if err != nil {
			log.Fatalf("Configuration file %v not readable", *config)
		}
		confstring = string(b)
	}

	r, err := relay.ParseRelay(confstring)
	if err != nil {
		log.Fatalf("Error parsing configuration: %v", err)
	}

	// sanity checks
	if len(r.Backend.Endpoints) < 1 {
		log.Fatalf("No backend endpoints available")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	log.Printf("Starting backend clients")
	go r.Backend.Run()

	go func() {
		log.Printf("Starting server on address: %v\n", r.Listener.Addr)
		err := r.Listener.Run()
		if err != nil {
			log.Panicf("Error detected while running HTTP server: %v", err)
		}
		wg.Done()
	}()

	signal.Notify(c, os.Interrupt)
	select {
	case <-c:
		// received ^C
		log.Printf("Caught: INTERRUPT. Stopping...")
		r.Stop()
	}
	wg.Wait()

}
