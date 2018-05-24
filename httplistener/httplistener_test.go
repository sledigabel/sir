package httplistener_test

import (
	"testing"

	"github.com/sledigabel/sir/httplistener"
)

func TestNewHTTPWithParameters(t *testing.T) {

	h := httplistener.NewHTTPWithParameters("http://testaddr:9999", "", "", 60)
	if h.Addr != "http://testaddr:9999" {
		t.Fatalf("Could not create HTTP with proper parameters")
	}
	if h.Timeout != 60 {
		t.Fatalf("Could not create HTTP with proper parameters")
	}
}

func TestNewHTTPfromConfig(t *testing.T) {

	hc := &httplistener.HTTPConf{
		Addr:    "http://testaddr:9898",
		Timeout: 60,
	}
	h := httplistener.NewHTTPfromConfig(hc)
	if h.Addr != "http://testaddr:9898" {
		t.Fatalf("Address is incorrect from config")
	}
	if h.Timeout != 60 {
		t.Fatalf("Timeout is incorrect from config")
	}
}

func TestNewHTTPConfParser(t *testing.T) {

	testconf := `
	[listener]
	addr = "http://testad:9797"
	certificate = "/opt/pki/test.pem"
	retention_policy = "any"
	timeout = 5
	`

	hc, err := httplistener.NewHTTPParseConfig(testconf)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}
	if hc.Addr != "http://testad:9797" {
		t.Errorf("Address is incorrect from parsed config\nGot: %v", hc.Addr)
	}
	if hc.Timeout != 5 {
		t.Errorf("Timeout is incorrect from parsed config")
	}
	if hc.Certificate != "/opt/pki/test.pem" {
		t.Errorf("Certificate is incorrect from parsed config")
	}
	if hc.RetentionPolicy != "any" {
		t.Errorf("Retention policy is incorrect from parsed config")
	}
}
