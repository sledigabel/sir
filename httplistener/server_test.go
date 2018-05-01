package httplistener_test

import (
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/sledigabel/sir/httplistener"
)

var mutex sync.Mutex

func TestNewServerStartStop(t *testing.T) {

	mutex.Lock()
	t.Log("Starting server")
	h := httplistener.NewHTTP()
	h.Addr = "localhost:19999"

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := h.Run()
		if err != nil {
			t.Fatalf("Found an error with server: %v", err)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)
	t.Log("Pinging server")
	resp, err := http.Get("http://localhost:19999/ping")
	if err != nil {
		t.Fatalf("Cannot http-ping localhost, %v", err)
	}
	t.Logf("Headers: %v\nStatus: %v", resp.Header, resp.Status)
	t.Log("Stopping server")
	h.Stop()
	wg.Wait()
	mutex.Unlock()

}

func TestGetWrongPath(t *testing.T) {

	mutex.Lock()
	t.Log("Starting server")
	h := httplistener.NewHTTPWithParameters("localhost:19990", "", "", 180)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := h.Run()
		if err != nil {
			t.Fatalf("Found an error with server: %v", err)
		}
		wg.Done()
	}()
	// stop after a while

	time.Sleep(time.Second)
	t.Log("Accessing wrong endpoint")
	resp, err := http.Get("http://localhost:19990/test")
	if err != nil {
		t.Fatalf("Can't connect to server")
	}

	t.Logf("Headers: %v\nStatus: %v", resp.Header, resp.Status)
	if resp.StatusCode/100 != 4 {
		t.Fatalf("Should fail connecting to /test endpoint")
	}
	h.Stop()
	t.Log("Stopping server")
	wg.Wait()
	t.Log("Stopped")
	mutex.Unlock()
}

func TestSuccessfulWriteInfluxClient(t *testing.T) {

	t.Log("Starting server")
	h := httplistener.NewHTTP()
	h.Addr = "localhost:19999"
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := h.Run()

		if err != nil {
			t.Fatalf("Found an error with server: %v", err)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:19999",
	})
	if err != nil {
		t.Fatalf("Cannot connect to the server using influx client")
	}

	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: "testdb",
	})

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	pt, _ := client.NewPoint("cpu_usage", tags, fields, time.Now())
	bp.AddPoint(pt)
	c.Write(bp)

	t.Log("Stopping server")
	h.Stop()
	wg.Wait()

}

func TestMissingDB(t *testing.T) {
	t.Log("Starting server")
	h := httplistener.NewHTTP()
	h.Addr = "localhost:19992"
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := h.Run()
		if err != nil {
			t.Fatalf("Found an error with server: %v", err)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)
	t.Log("Connecting without db")

	resp, err := http.Post("http://localhost:19992/write", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatalf("Can't connect to server")
	}
	defer resp.Body.Close()
	s, _ := ioutil.ReadAll(resp.Body)
	t.Logf("Headers: %v\nStatus: %v\nBody: %v", resp.Header, resp.Status, string(s))
	if resp.StatusCode/100 != 4 {
		t.Fatalf("Should fail, missing db parameter")
	}
	t.Log("Stopping server")
	h.Stop()
	wg.Wait()

}

func TestRubbishData(t *testing.T) {
	t.Log("Starting server")
	h := httplistener.NewHTTP()
	h.Addr = "localhost:19991"
	h.Debug = true
	h.DebugConnections = true
	h.Timeout = 99
	wg := sync.WaitGroup{}
	wg.Add(1)

	t.Logf("%v", h)

	go func() {
		err := h.Run()
		if err != nil {
			t.Fatalf("Found an error with server: %v", err)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)
	t.Log("Sending rubbish data")

	data := strings.NewReader("No chance\nYou can--PaRse/This")
	resp, err := http.Post("http://localhost:19991/write?db=test", "application/json", data)
	if err != nil {
		t.Fatalf("Can't connect to server")
	}
	defer resp.Body.Close()
	s, _ := ioutil.ReadAll(resp.Body)
	t.Logf("Headers: %v\nStatus: %v\nBody: %v", resp.Header, resp.Status, string(s))
	if resp.StatusCode/100 != 4 {
		t.Fatalf("Should fail, rubbish data")
	}
	t.Log("Stopping server")
	h.Stop()

	wg.Wait()
}
