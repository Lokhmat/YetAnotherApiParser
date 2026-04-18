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

func (f fakePlanner) GenerateCyclePlan(ctx context.Context, spec *openapi3.T, baseURL string, _ core.MigrationTarget) (*core.CyclePlan, error) {
	var (
		upsertPlan   *core.MigrationPlan
		fullSyncPlan *core.FullSyncPlan
		err          error
	)
	if f.generatePlan != nil {
		upsertPlan, err = f.generatePlan(ctx, spec, baseURL)
		if err != nil {
			return nil, err
		}
	}
	if f.generateFullSync != nil {
		fullSyncPlan, err = f.generateFullSync(ctx, spec, baseURL)
		if err != nil {
			return nil, err
		}
	}
	return &core.CyclePlan{UpsertPlan: upsertPlan, FullSyncPlan: fullSyncPlan}, nil
}

func (fakePlanner) CommitCycle(*core.CyclePlan) {}

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

func (f fakeTarget) LoadCheckpoint(context.Context, string) (*core.Checkpoint, error) {
	return nil, nil
}

func (f fakeTarget) SaveCheckpoints(context.Context, []core.Checkpoint) error {
	return nil
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
	if !strings.Contains(out.String(), "Applied 3 statements") {
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
			CycleEnabled:         true,
			CycleInterval:        time.Second,
			CycleIntervalSeconds: 1,
		},
		Database: config.DatabaseConfig{Provider: "postgres"},
		Control:  config.ControlConfig{HistoryLimit: 2},
	}, nil, fakePlanner{
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
		select {
		case <-sleepRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
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
	if !strings.Contains(out.String(), "Cycle 1 applied 2 statements") {
		t.Fatalf("unexpected periodic output: %s", out.String())
	}
}

func TestRunnerTriggerCycleWakesSleepingRunner(t *testing.T) {
	var out bytes.Buffer
	tracker := NewRequestTracker()
	sleepStarted := make(chan struct{}, 1)
	cycleStarted := make(chan int, 4)
	cycle := 0
	r := New(config.Config{
		API: config.APIConfig{BaseURL: "https://api.example.com"},
		Runtime: config.RuntimeConfig{
			CycleEnabled:         true,
			CycleInterval:        time.Hour,
			CycleIntervalSeconds: 3600,
		},
		Database: config.DatabaseConfig{Provider: "postgres"},
		Control:  config.ControlConfig{HistoryLimit: 4},
	}, nil, fakePlanner{
		generateFullSync: func(context.Context, *openapi3.T, string) (*core.FullSyncPlan, error) {
			cycle++
			cycleStarted <- cycle
			return &core.FullSyncPlan{
				Tables: []core.FullSyncTable{{
					Name: "items",
					Rows: []core.InsertRow{{}},
				}},
			}, nil
		},
	}, fakeTarget{
		fullSyncResult: core.ApplyResult{AppliedCount: 1},
	}, &out, tracker, observability.NopEventLogger{}, func(string, []byte, fs.FileMode) error { return nil }, func(ctx context.Context, _ time.Duration) error {
		select {
		case sleepStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx)
	}()

	if got := <-cycleStarted; got != 1 {
		t.Fatalf("expected first cycle to start, got %d", got)
	}
	<-sleepStarted

	status := r.StatusSnapshot()
	if status.Phase != PhaseSleeping {
		t.Fatalf("expected sleeping status before manual trigger, got %+v", status)
	}
	if err := r.TriggerCycle(); err != nil {
		t.Fatalf("TriggerCycle returned error: %v", err)
	}
	if got := <-cycleStarted; got != 2 {
		t.Fatalf("expected second cycle after manual trigger, got %d", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}
