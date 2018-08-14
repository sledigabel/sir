package endpoint_test

import (
	"os"
	"testing"
	"time"

	"github.com/sledigabel/sir/influx-endpoint"
)

func TestEndpointMgmtNew(t *testing.T) {
	n := endpoint.NewHTTPInfluxServerMgr()

	if len(n.Endpoints) != 0 {
		t.Error("Failed creating empty HTTPInfluxServerMgr")
	}
}

func TestEndpointMgmtNewFromConfig(t *testing.T) {
	var config string = `
	debug = true
	
	[servers]

		[server.1]
		alias = "test1"
		db_regex = [".*"]
		port = 9090

		[server.2]
		alias = "test2"
		unsafe_ssl = true

	[internal]
	enable = true
	database = "test"
	frequency = "45s"
	`
	n, err := endpoint.NewHTTPInfluxServerMgrFromConfig(config)
	t.Logf("%v\n", n)
	t.Logf("%v\n", err)
	if err != nil || len(n.Endpoints) != 2 {
		t.Fatalf("The 2 servers were not created properly: %v", err)
	}

	s1, err := n.GetServerPerName("test1")
	if err != nil || s1.Alias != "test1" {
		t.Fatalf("The search failed on test1: %v", err)
	}

	_, err = n.GetServerPerName("test3")
	if err == nil {
		t.Fatalf("Found a non-existent server (test3)!")
	}

	if !n.Telemetry.Enable ||
		n.Telemetry.Database != "test" ||
		n.Telemetry.Frequency != "45s" {
		t.Fatalf("Did not find the expected results: %v", n.Telemetry)
	}

	if n.Debug != true {
		t.Fatalf("Did not set up Debug: %v", n.Debug)
	}
}

func TestEndpointMgmtNewFromConfigDuplicates(t *testing.T) {
	var config string = `
	[servers]

		[server.1]
		alias = "test1"
		port = 9090
		enable = false

		[server.2]
		alias = "test1"
		unsafe_ssl = true
		enable = false

	`
	_, err := endpoint.NewHTTPInfluxServerMgrFromConfig(config)
	if err == nil {
		t.Fatalf("I was expecting a failure here! server test1 is duplicated")
	}

}

func TestEndpointMgmtNewRun(t *testing.T) {

	var config string = `
	[servers]
		[server.1]
		alias = "test1"
		enable = false

		[server.2]
		alias = "test2"
		enable = false
	`
	mgr, err := endpoint.NewHTTPInfluxServerMgrFromConfig(config)
	if err != nil {
		t.Fatalf("Error creating the 2 servers")
	}

	go mgr.Run()
	time.Sleep(time.Second)
	s := string(mgr.Status())
	if s != "{\"test1\":\"failed\",\"test2\":\"failed\"}" {
		t.Fatalf("Status check failed: %v", s)
	}
	t.Logf("Status: %v", s)
	mgr.Shutdown <- struct{}{}
	t.Log("Shutdown Completed")

}

func TestEndpointMgmtNewPost(t *testing.T) {

	ts := emptyTestServer()
	defer ts.Close()

	var config string = `
	[servers]
		[server.1]
		alias = "simple"
	`
	mgr, err := endpoint.NewHTTPInfluxServerMgrFromConfig(config)
	if err != nil {
		t.Fatalf("Error creating server")
	}
	_, ok := mgr.Endpoints["simple"]
	if !ok {
		t.Fatalf("simple not in endpoint list")
	}
	mgr.Endpoints["simple"].Config.Addr = ts.URL
	t.Logf("Endpoint: %v", ts.URL)

	go mgr.Run()
	time.Sleep(100 * time.Millisecond)
	pts := createBatch()
	err = mgr.Post(pts)
	if err != nil {
		t.Fatalf("Failed posting example points: %v", err)
	}

	time.Sleep(time.Second)
	mgr.Shutdown <- struct{}{}
	t.Log("Shutdown Completed")

}

func TestEndpointMgmtNewStats(t *testing.T) {

	var config string = `
	[servers]
		[server.1]
		alias = "test1"
		disable = true
		buffering = true

		[server.2]
		alias = "test2"
		disable = true
	`
	mgr, err := endpoint.NewHTTPInfluxServerMgrFromConfig(config)
	if err != nil {
		t.Fatalf("Error creating the 2 servers")
	}
	go mgr.Run()
	bp, err := mgr.Stats()
	if err != nil {
		t.Fatalf("Couldn't get stats: %v", err)
	}
	if len(bp.Points()) < 2 {
		t.Fatal("Collected too few metrics :-(")
	}
	for _, p := range bp.Points() {
		t.Log(p.String())
	}
	mgr.Shutdown <- struct{}{}
	t.Log("Shutdown Completed")
	os.RemoveAll("./test1")

}

func TestEndpointMgmtNewSubmitStats(t *testing.T) {
	ts := emptyTestServer()
	defer ts.Close()

	var conf_submit_stats string = `
	[internal]
	database = "test_submit"
	frequency = "100ms"
	enable = true

	[servers]
		[server.1]
		alias = "test1"
		enable = false
	`

	mgr_submit, err := endpoint.NewHTTPInfluxServerMgrFromConfig(conf_submit_stats)
	mgr_submit.Endpoints["test1"].Config.Addr = ts.URL

	go mgr_submit.Run()
	time.Sleep(1 * time.Second)

	bp, err := mgr_submit.Stats()
	if err != nil {
		t.Fatalf("Couldn't get stats: %v", err)
	}
	if len(bp.Points()) < 1 {
		t.Fatal("Collected too few metrics :-(")
	}
	fields, _ := bp.Points()[0].Fields()
	if bp.Points()[0].Name() != "sir_backend" || fields["posted"].(int64) < 9 {
		t.Fatalf("Metrics were not collected enough:\n%v", bp.Points()[0].String())
	}

	s := string(mgr_submit.Status())
	if s != "{\"test1\":\"active\"}" {
		t.Fatalf("Status check failed: %v", s)
	}
	mgr_submit.Shutdown <- struct{}{}
	t.Log("Shutdown Completed")

}
