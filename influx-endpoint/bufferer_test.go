package endpoint_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/sledigabel/sir/influx-endpoint"
)

func TestNewBuffererDefault(t *testing.T) {

	b := endpoint.NewBufferer()
	if dir, _ := os.Getwd(); dir != b.RootPath {
		t.Error("Default Bufferer not set to current dir")
	}

}

func TestNewBuffererDifferentRootPath(t *testing.T) {

	b := endpoint.NewBufferer()
	dir, err := ioutil.TempDir("", "bufferer")
	if err != nil {
		t.Fatalf("Could not create tmpdir: %v", err)
	}

	defer os.RemoveAll(dir) // clean up
	t.Logf("Using tmpdir: %v", dir)

	b.RootPath = dir
	if err := b.Init(); err != nil {
		t.Fatalf("Could not init Bufferer: %v", err)
	}

}

func TestNewBuffererWrite(t *testing.T) {

	b := endpoint.NewBufferer()
	dir, err := ioutil.TempDir("", "bufferer")
	if err != nil {
		t.Fatalf("Could not create tmpdir: %v", err)
	}

	defer os.RemoveAll(dir) // clean up
	b.RootPath = dir

	if err := b.Init(); err != nil {
		t.Fatalf("Could not init Bufferer: %v", err)
	}

	bp := createBatch()

	if err := b.Write(bp); err != nil {
		t.Fatalf("Could not Write batch: %v", err)
	}

	t.Logf("State of Index: %v => %v", b.Index, b.Index[0])
	if len(b.Index) < 1 {
		t.Fatalf("Could not add index: %v", err)
	}
	if _, err = os.Stat(filepath.Join(b.RootPath, b.Index[0].Filename)); err != nil {
		t.Fatalf("Did not create a file: %v", err)
	}

}

func TestNewBuffererFlush(t *testing.T) {

	b := endpoint.NewBufferer()
	dir, err := ioutil.TempDir("", "bufferer")
	if err != nil {
		t.Fatalf("Could not create tmpdir: %v", err)
	}

	defer os.RemoveAll(dir) // clean up
	b.RootPath = dir

	if err := b.Init(); err != nil {
		t.Fatalf("Could not init Bufferer: %v", err)
	}

	bp_1a := createBatch()
	bp_1b := createBatch()
	bp_1c := createBatch()
	bp_2a := createBatch()
	bp_2a.SetDatabase("Wasp")
	bp_2b := createBatch()
	bp_2b.SetDatabase("Wasp")
	b.Input <- bp_1a
	b.Input <- bp_2a
	b.Input <- bp_1b
	b.Input <- bp_1c
	b.Input <- bp_2b
	if err := b.Flush(); err != nil {
		t.Fatalf("Could not flush Bufferer: %v", err)
	}

	t.Logf("State of Index: %v => [%v %v]", b.Index, b.Index[0], b.Index[1])
	if len(b.Index) < 2 {
		t.Fatalf("Could not add index: %v", err)
	}
	if b.Index[0].NumMetrics != 3 && b.Index[1].NumMetrics != 2 {
		t.Fatalf("Improper batching")
	}
	if _, err = os.Stat(filepath.Join(b.RootPath, b.Index[0].Filename)); err != nil {
		t.Fatalf("Did not create a file: %v", err)
	}

}
