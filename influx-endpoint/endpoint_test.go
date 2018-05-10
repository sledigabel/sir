package endpoint_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
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

func TestGetInfluxServerbyDBBasic(t *testing.T) {

	// setup
	var list []*endpoint.HTTPInfluxServer
	list = make([]*endpoint.HTTPInfluxServer, 3)
	list[0], _ = endpoint.NewHTTPInfluxServer(
		"test1",
		[]string{".*"},
		&client.HTTPConfig{})

	list[1], _ = endpoint.NewHTTPInfluxServer(
		"test2",
		[]string{"SHOULDNEVERMATCH"},
		&client.HTTPConfig{})
	list[2], _ = endpoint.NewHTTPInfluxServer(
		"test3",
		[]string{"[a-z]*"},
		&client.HTTPConfig{})

	filtered := endpoint.GetInfluxServerbyDB("try", list)
	if len(filtered) != 2 {
		for _, s := range filtered {
			t.Logf("Matched on: %v", s.Alias)
		}
		t.Errorf("Should only match 2 servers but found %v", len(filtered))
	}
}

func emptyTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		log.Printf("Received: %v\nContent: %v", r, string(b))
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("X-Influxdb-Version", "x.x")
	}))
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
	c.PingFreq = 100 * time.Millisecond
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
	t.Log("Sending some points")
	err = c.Post(createBatch())
	if err != nil {
		t.Fatalf("Unable to post dummy data")
	}
	pt, err := c.Stats()
	if err != nil {
		t.Fatalf("Unable to read stats on server: %v", c.Alias)
	}
	t.Logf("Stats: %v", pt)
	if string(pt.Name()) != "sir_relay" {
		t.Fatalf("Statistic has the wrong name: %v", pt.Name())
	}
	// sending shutdown
	t.Log("Sending shutdown msg")
	c.Shutdown <- struct{}{}
	wg.Wait()
	t.Log("Completed shutdown")
}
