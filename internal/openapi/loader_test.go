package openapi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValidSpec(t *testing.T) {
	path := writeTempSpec(t, `{
  "openapi": "3.0.3",
  "info": {"title": "test", "version": "1.0.0"},
  "paths": {}
}`)

	spec, err := Load(context.Background(), path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if spec == nil || spec.Info == nil || spec.Info.Title != "test" {
		t.Fatalf("unexpected spec: %+v", spec)
	}
}

func TestLoadMissingFileReturnsError(t *testing.T) {
	_, err := Load(context.Background(), filepath.Join(t.TempDir(), "missing.json"))
	if err == nil {
		t.Fatal("expected missing file error")
	}
}

func TestLoadInvalidSpecReturnsError(t *testing.T) {
	path := writeTempSpec(t, `{"openapi":`)

	_, err := Load(context.Background(), path)
	if err == nil {
		t.Fatal("expected invalid spec error")
	}
}

func writeTempSpec(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return path
}
