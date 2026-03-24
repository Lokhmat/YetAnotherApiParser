package runner

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/observability"
	"github.com/getkin/kin-openapi/openapi3"
)

type fakePlanner struct {
	generatePlan     func(context.Context, *openapi3.T, string) (*core.MigrationPlan, error)
	generateFullSync func(context.Context, *openapi3.T, string) (*core.FullSyncPlan, error)
}

func (f fakePlanner) GeneratePlan(ctx context.Context, spec *openapi3.T, baseURL string) (*core.MigrationPlan, error) {
	return f.generatePlan(ctx, spec, baseURL)
}

func (f fakePlanner) GenerateFullSyncPlan(ctx context.Context, spec *openapi3.T, baseURL string) (*core.FullSyncPlan, error) {
	return f.generateFullSync(ctx, spec, baseURL)
}

type fakeTarget struct {
	applyResult       core.ApplyResult
	applyErr          error
	fullSyncResult    core.ApplyResult
	fullSyncErr       error
	exportSQL         []byte
	exportFullSyncSQL []byte
}

func (f fakeTarget) Apply(context.Context, *core.MigrationPlan) (core.ApplyResult, error) {
	return f.applyResult, f.applyErr
}

func (f fakeTarget) ApplyFullSync(context.Context, *core.FullSyncPlan) (core.ApplyResult, error) {
	return f.fullSyncResult, f.fullSyncErr
}

func (f fakeTarget) ExportSQL(*core.MigrationPlan) ([]byte, error) {
	return f.exportSQL, nil
}

func (f fakeTarget) ExportFullSyncSQL(*core.FullSyncPlan) ([]byte, error) {
	return f.exportFullSyncSQL, nil
}

func (f fakeTarget) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true, CanFullSync: true}
}

func TestRunnerOneShotSuccessTracksCountsAndCompletion(t *testing.T) {
	var out bytes.Buffer
	tracker := NewRequestTracker()
	r := New(config.Config{
		API:     config.APIConfig{BaseURL: "https://api.example.com"},
		Control: config.ControlConfig{HistoryLimit: 5},
	}, nil, fakePlanner{
		generatePlan: func(context.Context, *openapi3.T, string) (*core.MigrationPlan, error) {
			tracker.LogRequest(observability.RequestEvent{StatusCode: 200})
			tracker.LogRequest(observability.RequestEvent{StatusCode: 500, Err: errors.New("retry")})
			return &core.MigrationPlan{
				Operations: []core.MigrationOperation{
					core.CreateTableOp{TableName: "items"},
					core.InsertRowsOp{
						TableName: "items",
						Rows:      []core.InsertRow{{}, {}},
					},
				},
			}, nil
		},
		generateFullSync: func(context.Context, *openapi3.T, string) (*core.FullSyncPlan, error) {
			return nil, errors.New("unexpected")
		},
	}, fakeTarget{
		exportSQL:   []byte("SELECT 1;"),
		applyResult: core.ApplyResult{AppliedCount: 3},
	}, &out, tracker, observability.NopEventLogger{}, func(string, []byte, fs.FileMode) error { return nil }, func(context.Context, time.Duration) error { return nil })

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	status := r.StatusSnapshot()
	if status.Phase != PhaseCompleted || status.RequestCount != 2 || status.FailedRequestCount != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.ManagedTableCount != 1 || status.PlannedRowCount != 2 || status.AppliedStatementCount != 3 {
		t.Fatalf("unexpected derived metrics: %+v", status)
	}
	if status.NextRunAt != nil || status.LastSuccessAt == nil {
		t.Fatalf("unexpected completion timestamps: %+v", status)
	}
	runs := r.RunsSnapshot()
	if len(runs) != 1 || runs[0].Outcome != RunOutcomeSucceeded {
		t.Fatalf("unexpected history: %+v", runs)
	}
	if !strings.Contains(out.String(), "Applied 3 migrations") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestRunnerOneShotApplyFailureMarksFailedAndKeepsCompatibility(t *testing.T) {
	var wrotePath string
	var out bytes.Buffer
	tracker := NewRequestTracker()
	r := New(config.Config{
		API:     config.APIConfig{BaseURL: "https://api.example.com"},
		Runtime: config.RuntimeConfig{SQLOutputPath: "fallback.sql"},
		Control: config.ControlConfig{HistoryLimit: 5},
	}, nil, fakePlanner{
		generatePlan: func(context.Context, *openapi3.T, string) (*core.MigrationPlan, error) {
			tracker.LogRequest(observability.RequestEvent{StatusCode: 200})
			return &core.MigrationPlan{
				Operations: []core.MigrationOperation{core.InsertRowsOp{TableName: "items", Rows: []core.InsertRow{{}}}},
			}, nil
		},
		generateFullSync: func(context.Context, *openapi3.T, string) (*core.FullSyncPlan, error) {
			return nil, errors.New("unexpected")
		},
	}, fakeTarget{
		exportSQL: []byte("INSERT INTO items VALUES (1);"),
		applyErr:  errors.New("db unavailable"),
	}, &out, tracker, observability.NopEventLogger{}, func(path string, _ []byte, _ fs.FileMode) error {
		wrotePath = path
		return nil
	}, func(context.Context, time.Duration) error { return nil })

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	status := r.StatusSnapshot()
	if status.Phase != PhaseFailed || status.LastError == "" {
		t.Fatalf("expected failed status, got %+v", status)
	}
	if wrotePath != "fallback.sql" {
		t.Fatalf("expected fallback SQL write, got %q", wrotePath)
	}
	runs := r.RunsSnapshot()
	if len(runs) != 1 || runs[0].Outcome != RunOutcomeFailed {
		t.Fatalf("unexpected history after failure: %+v", runs)
	}
}

func TestRunnerPeriodicTracksSleepingNextRunAndHistoryLimit(t *testing.T) {
	var out bytes.Buffer
	tracker := NewRequestTracker()
	sleepStarted := make(chan struct{}, 1)
	sleepRelease := make(chan struct{})
	cycle := 0
	r := New(config.Config{
		API: config.APIConfig{BaseURL: "https://api.example.com"},
		Runtime: config.RuntimeConfig{
			FullReloadEnabled:         true,
			FullReloadInterval:        time.Second,
			FullReloadIntervalSeconds: 1,
		},
		Database: config.DatabaseConfig{Provider: "postgres"},
		Control:  config.ControlConfig{HistoryLimit: 2},
	}, nil, fakePlanner{
		generatePlan: func(context.Context, *openapi3.T, string) (*core.MigrationPlan, error) {
			return nil, errors.New("unexpected")
		},
		generateFullSync: func(context.Context, *openapi3.T, string) (*core.FullSyncPlan, error) {
			cycle++
			tracker.LogRequest(observability.RequestEvent{StatusCode: 200})
			return &core.FullSyncPlan{
				Tables: []core.FullSyncTable{{
					Name: "items",
					Rows: []core.InsertRow{{}, {}},
				}},
			}, nil
		},
	}, fakeTarget{
		fullSyncResult: core.ApplyResult{AppliedCount: 2},
	}, &out, tracker, observability.NopEventLogger{}, func(string, []byte, fs.FileMode) error { return nil }, func(ctx context.Context, _ time.Duration) error {
		select {
		case sleepStarted <- struct{}{}:
		default:
		}
		if cycle >= 3 {
			return context.Canceled
		}
		<-sleepRelease
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- r.Run(context.Background())
	}()

	<-sleepStarted
	status := r.StatusSnapshot()
	if status.Phase != PhaseSleeping || status.NextRunAt == nil {
		t.Fatalf("expected sleeping status with next run, got %+v", status)
	}
	close(sleepRelease)
	err := <-done
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	runs := r.RunsSnapshot()
	if len(runs) != 2 {
		t.Fatalf("expected history to be trimmed to 2, got %+v", runs)
	}
	if runs[0].Cycle != 3 || runs[1].Cycle != 2 {
		t.Fatalf("unexpected trimmed history order: %+v", runs)
	}
	if !strings.Contains(out.String(), "Full reload cycle 1 applied 2 statements") {
		t.Fatalf("unexpected periodic output: %s", out.String())
	}
}
