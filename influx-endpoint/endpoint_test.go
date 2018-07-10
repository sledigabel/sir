package endpoint_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/sledigabel/sir/influx-endpoint"
)

func TestNewHTTPInfluxServerBasic(t *testing.T) {
	_, err := endpoint.NewHTTPInfluxServer("test", []string{"test"}, &client.HTTPConfig{})
	if err != nil {
		t.Errorf("Error creating basic Influx Endpoint: %v", err)
	}
}

func TestNewHTTPInfluxServerEmptyAlias(t *testing.T) {
	_, err := endpoint.NewHTTPInfluxServer("", []string{"test"}, &client.HTTPConfig{})
	if err == nil {
		t.Errorf("Error, should fail for empty alias: %v", err)
	}
}

func TestNewHTTPInfluxServerEmptyDBRegex(t *testing.T) {
	i, err := endpoint.NewHTTPInfluxServer("test", []string{""}, &client.HTTPConfig{})
	if err != nil {
		t.Errorf("Error, should not fail for empty regex: %v", err)
	}
	if len(i.Dbregex) != 1 && i.Dbregex[0] != ".*" {
		t.Errorf("Error, Dbregex should have been replaced by a .*: %v", err)
	}
}

func TestNewHTTPInfluxServerConnect(t *testing.T) {

	ts := emptyTestServer()
	defer ts.Close()
	c, err := endpoint.NewHTTPInfluxServer(
		"test", []string{"test"}, &client.HTTPConfig{Addr: ts.URL})
	if err != nil {
		t.Errorf("Couldn't connect on empty config: %v", err)
	}
	err = c.Connect()
	t.Logf("Server status after connect: %v", c.Status)
	if err != nil {
		t.Errorf("Failed connecting: %v", err)
	}
	if c.Status != endpoint.ServerStateActive {
		t.Errorf("The server should be active")
	}
	c.Close()
	t.Logf("Server status after close: %v", c.Status)
	if c.Status != endpoint.ServerStateInactive {
		t.Errorf("The server should be inactive")
	}

}

func TestNewHTTPInfluxServerFromConfig(t *testing.T) {
	config := `
	server_name = "test"
	alias = "test"
	db_regex = [".*"]
	username = "seb"
	password = "S3kR3t"
	precision = "superfine"
	write_consistency = "any"
	port = 9090
	timeout = "1m"
	unsafe_ssl = true
	secure = false
	`
	conf, err := endpoint.NewHTTPInfluxServerParseConfig(config)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}
	server := endpoint.NewHTTPInfluxServerFromConfig(conf)
	if server.Alias != "test" || server.Config.Addr != "http://test:9090" || server.Dbregex[0] != ".*" || server.Config.InsecureSkipVerify != true {
		t.Fatalf("Error building server from config")
	}

}

func TestHTTPInfluxServerRun(t *testing.T) {

	var wg sync.WaitGroup
	ts := emptyTestServer()
	defer ts.Close()
	c, err := endpoint.NewHTTPInfluxServer(
		"test", []string{"test"}, &client.HTTPConfig{Addr: ts.URL})
	if err != nil {
		t.Errorf("Couldn't connect on empty config: %v", err)
	}
	wg.Add(1)
	go func() {
		c.Run()
		wg.Done()
	}()
	// wait 5s to simulate some activity
	time.Sleep(1 * time.Second)
	// sending shutdown
	t.Log("Sending shutdown msg")
	c.Shutdown <- struct{}{}
	wg.Wait()
	t.Log("Completed shutdown")
}

func createBatch() client.BatchPoints {
	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{})
	bp.SetDatabase("BumbleBeeTuna")
	bp.SetPrecision("ms")

	// Create a point and add to batch
	tags := models.NewTags(map[string]string{"cpu": "cpu-total"})
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	pt, err := models.NewPoint("cpu_usage", tags, fields, time.Now())
	if err != nil {
		fmt.Println("Error: ", err.Error())
	}
	bp.AddPoint(client.NewPointFrom(pt))
	return bp
}

func TestEndpointWrite(t *testing.T) {
	var wg sync.WaitGroup

	ts := emptyTestServer()
	defer ts.Close()

	c, err := endpoint.NewHTTPInfluxServer(
		"test", []string{"test"}, &client.HTTPConfig{Addr: ts.URL})
	t.Logf("Endpoint: %v", ts.URL)

	c.PingFreq = time.Second
	if err != nil {
		t.Errorf("Couldn't connect on empty config: %v", err)
	}
	wg.Add(1)
	go func() {
		err := c.Run()
		if err != nil {
			t.Errorf("Error when running endpoint: %v", err)
		}
		wg.Done()
	}()
	// wait to simulate some activity
	c.Ping()
	for c.Status != endpoint.ServerStateActive {
		time.Sleep(300 * time.Millisecond)
		err = c.Ping()
	}

	time.Sleep(100 * time.Millisecond)
	t.Logf("Sending some points, status: %v", c.Status)
	err = c.Post(createBatch())
	if err != nil {
		t.Errorf("Unable to post dummy data: %v", err)
	}
	pt, err := c.Stats()
	if err != nil {
		t.Errorf("Unable to read stats on server %v: %v", c.Alias, err)
	}
	if len(pt) > 0 && string(pt[0].Name()) != "sir_backend" {
		t.Errorf("Statistic has the wrong name: %v", pt[0].Name())
	}
	// sending shutdown
	c.Shutdown <- struct{}{}
	wg.Wait()
}

func TestEndpointWriteFailed(t *testing.T) {
	var wg sync.WaitGroup
	c, err := endpoint.NewHTTPInfluxServer(
		"test", []string{"test"}, &client.HTTPConfig{Addr: "http://127.0.0.1:12345"})
	c.PingFreq = 100 * time.Millisecond
	if err != nil {
		t.Errorf("Couldn't connect on empty config: %v", err)
	}
	wg.Add(1)
	go func() {
		err := c.Run()
		if err != nil {
			t.Errorf("Error when running endpoint: %v", err)
		}
		wg.Done()
	}()
	t.Log("Sending some points")
	err = c.Post(createBatch())
	if err == nil {
		t.Errorf("Should have failed posting dummy data: %v", err)
	}
	_, err = c.Stats()
	if err != nil {
		t.Errorf("Unable to read stats on server %v: %v", c.Alias, err)
	}
	// sending shutdown
	c.Shutdown <- struct{}{}
	wg.Wait()
}

func TestEndpointHTTPEndpointEmptyBufferer(t *testing.T) {

	config := `
	server_name = "test"
	alias = "test"
	`
	hc, err := endpoint.NewHTTPInfluxServerParseConfig(config)
	if err != nil {
		t.Errorf("Could not parse config: %v", err)
	}
	h := endpoint.NewHTTPInfluxServerFromConfig(hc)
	if h.Buffering || h.Bufferer != nil {
		t.Error("Error: Buffering created or activated")
	}
}

func TestEndpointHTTPEndpointEnabledBuffererDefaultPath(t *testing.T) {

	config := `
	server_name = "test"
	alias = "test"
	buffering = true
	`
	hc, err := endpoint.NewHTTPInfluxServerParseConfig(config)
	if err != nil {
		t.Errorf("Could not parse config: %v", err)
	}
	h := endpoint.NewHTTPInfluxServerFromConfig(hc)
	if h.Buffering == false || h.Bufferer == nil {
		t.Error("Error: Buffering not created or activated")
	}

	if h.Bufferer.RootPath != h.Alias {
		t.Error("Default path not set to alias")
	}

}

func TestEndpointHTTPEndpointEnabledBuffererDifferentPath(t *testing.T) {

	config := `
	server_name = "localhost"
	port = 12345
	alias = "test"
	ping_frequency = "100ms"
	buffering = true
	buffer_path = "blahblah"
	buffer_flush_frequency = "1s"
	`
	hc, err := endpoint.NewHTTPInfluxServerParseConfig(config)
	if err != nil {
		t.Errorf("Could not parse config: %v", err)
	}
	h := endpoint.NewHTTPInfluxServerFromConfig(hc)
	if h.Buffering == false || h.Bufferer == nil {
		t.Error("Error: Buffering not created or activated")
	}

	if h.Bufferer.RootPath != "blahblah/test" {
		t.Error("Default path not set correctly")
	}
	if h.Bufferer.FlushFrequency.Seconds() != 1 {
		t.Error("Wrong buffer flush frequency")
		t.Log(h.Bufferer)
	}

	defer os.RemoveAll("blahblah")
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		err := h.Run()
		if err != nil {
			t.Errorf("Error during endpoint run: %v", err)
		}
		wg.Done()
	}()

	// wait for server to be declared as failed
	for h.Status != endpoint.ServerStateFailed {
		time.Sleep(time.Second)
		fmt.Println(h.Status)
	}
	bp1 := createBatch()
	bp2 := createBatch()
	if err = h.Post(bp1); err != nil {
		t.Errorf("Should not catch exception here: %v", err)
	}
	if err = h.Post(bp2); err != nil {
		t.Errorf("Should not catch exception here: %v", err)
	}
	// wait here for flush
	time.Sleep(2 * time.Second)
	if len(h.Bufferer.Index) < 1 {
		t.Errorf("Bufferer did not populate: %v", h.Bufferer.Index)
	}
	// trigger a manual
	stop := make(chan struct{})
	go h.ProcessBacklog(stop)
	time.Sleep(time.Second)
	stop <- struct{}{}
	h.Shutdown <- struct{}{}
	wg.Wait()

}
