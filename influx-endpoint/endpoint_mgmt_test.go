package endpoint_test

import (
	"testing"

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
		db_regex = ".*"
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

}
