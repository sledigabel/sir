package endpoint_test

import (
	"sync"
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
	[servers]

		[server.1]
		alias = "test1"
		db_regex = [".*"]
		port = 9090

		[server.2]
		alias = "test2"
		unsafe_ssl = true

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

	var wg sync.WaitGroup
	wg.Add(1)
	go mgr.Run(&wg)
	time.Sleep(time.Second)
	t.Log("Shutdown mgr")
	mgr.Shutdown <- struct{}{}
	wg.Wait()
	t.Log("Shutdown Completed")

}
