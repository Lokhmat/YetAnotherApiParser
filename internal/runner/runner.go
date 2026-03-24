package runner

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"sync"
	"time"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/observability"
	"github.com/getkin/kin-openapi/openapi3"
)

type Phase string

const (
	PhaseStarting     Phase = "starting"
	PhaseRunningFetch Phase = "running_fetch"
	PhaseRunningApply Phase = "running_apply"
	PhaseSleeping     Phase = "sleeping"
	PhaseCompleted    Phase = "completed"
	PhaseFailed       Phase = "failed"
	PhaseStopping     Phase = "stopping"
)

type JobMode string

const (
	JobModeOneShot  JobMode = "one_shot"
	JobModePeriodic JobMode = "periodic"
)

type RunOutcome string

const (
	RunOutcomeSucceeded RunOutcome = "succeeded"
	RunOutcomeFailed    RunOutcome = "failed"
	RunOutcomeCanceled  RunOutcome = "canceled"
)

type Status struct {
	JobMode               JobMode    `json:"job_mode"`
	Phase                 Phase      `json:"phase"`
	Cycle                 int        `json:"cycle"`
	StartedAt             *time.Time `json:"started_at,omitempty"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
	NextRunAt             *time.Time `json:"next_run_at,omitempty"`
	RequestCount          int        `json:"request_count"`
	FailedRequestCount    int        `json:"failed_request_count"`
	ManagedTableCount     int        `json:"managed_table_count"`
	PlannedRowCount       int        `json:"planned_row_count"`
	AppliedStatementCount int        `json:"applied_statement_count"`
	LastError             string     `json:"last_error,omitempty"`
	LastSuccessAt         *time.Time `json:"last_success_at,omitempty"`
}

type RunSummary struct {
	Status
	Outcome RunOutcome `json:"outcome"`
}

type Planner interface {
	GeneratePlan(ctx context.Context, spec *openapi3.T, baseURL string) (*core.MigrationPlan, error)
	GenerateFullSyncPlan(ctx context.Context, spec *openapi3.T, baseURL string) (*core.FullSyncPlan, error)
}

type Runner struct {
	cfg         config.Config
	spec        *openapi3.T
	planner     Planner
	target      core.MigrationTarget
	out         io.Writer
	eventLogger observability.EventLogger
	tracker     *RequestTracker
	writeFile   func(string, []byte, fs.FileMode) error
	sleep       func(context.Context, time.Duration) error

	state *syncState
}

type syncState struct {
	mu         sync.RWMutex
	status     Status
	runHistory []RunSummary
	limit      int
}

func newSyncState(limit int) syncState {
	return syncState{limit: limit}
}

func New(cfg config.Config, spec *openapi3.T, planner Planner, target core.MigrationTarget, out io.Writer, tracker *RequestTracker, eventLogger observability.EventLogger, writeFile func(string, []byte, fs.FileMode) error, sleep func(context.Context, time.Duration) error) *Runner {
	mode := JobModeOneShot
	if cfg.Runtime.FullReloadEnabled {
		mode = JobModePeriodic
	}
	r := &Runner{
		cfg:         cfg,
		spec:        spec,
		planner:     planner,
		target:      target,
		out:         out,
		eventLogger: eventLogger,
		tracker:     tracker,
		writeFile:   writeFile,
		sleep:       sleep,
		state:       &syncState{limit: cfg.Control.HistoryLimit},
	}
	r.withState(func(state *syncState) {
		state.status = Status{
			JobMode: mode,
			Phase:   PhaseStarting,
		}
	})
	return r
}

func (r *Runner) Run(ctx context.Context) error {
	r.logEvent("info", "runner_started", "runner started", nil)

	if !r.cfg.Runtime.FullReloadEnabled {
		err := r.runOneShot(ctx)
		if err != nil {
			r.markFailure(err)
		}
		return err
	}

	if !r.target.Capabilities().CanFullSync {
		err := fmt.Errorf("db provider %q does not support periodic full reload", r.cfg.Database.Provider)
		r.markFailure(err)
		return err
	}

	fmt.Fprintf(r.out, "Periodic full reload enabled: every %ds\n", r.cfg.Runtime.FullReloadIntervalSeconds)

	for cycle := 1; ; cycle++ {
		err := r.runPeriodicCycle(ctx, cycle)
		if err != nil {
			if err == context.Canceled {
				r.setStopping()
				return nil
			}
			r.markFailure(err)
			return err
		}

		nextRunAt := time.Now().Add(r.cfg.Runtime.FullReloadInterval)
		r.withState(func(state *syncState) {
			state.status.Phase = PhaseSleeping
			state.status.NextRunAt = &nextRunAt
			state.status.FinishedAt = nil
		})
		if err := r.sleep(ctx, r.cfg.Runtime.FullReloadInterval); err != nil {
			if err == context.Canceled {
				r.setStopping()
				return nil
			}
			r.markFailure(err)
			return err
		}
	}
}

func (r *Runner) StatusSnapshot() Status {
	var snapshot Status
	r.withState(func(state *syncState) {
		snapshot = cloneStatus(state.status)
	})
	if snapshot.Phase == PhaseRunningFetch || snapshot.Phase == PhaseRunningApply {
		snapshot.RequestCount, snapshot.FailedRequestCount = r.tracker.Snapshot()
	}
	return snapshot
}

func (r *Runner) RunsSnapshot() []RunSummary {
	var runs []RunSummary
	r.withState(func(state *syncState) {
		runs = make([]RunSummary, len(state.runHistory))
		copy(runs, state.runHistory)
	})
	return runs
}

func (r *Runner) runOneShot(ctx context.Context) error {
	plan, metrics, err := r.buildOneShotPlan(ctx, 1)
	if err != nil {
		return err
	}

	sqlBytes, err := r.target.ExportSQL(plan)
	if err != nil {
		return r.failRun(1, err)
	}

	fmt.Fprintf(r.out, "\nGenerated %d operations:\n", len(plan.Operations))
	if len(sqlBytes) > 0 {
		fmt.Fprintf(r.out, "\n%s\n", string(sqlBytes))
	}

	r.updateApplyMetrics(1, metrics)
	result, err := r.target.Apply(ctx, plan)
	if err != nil {
		log.Printf("database apply failed: %v", err)
		r.logEvent("error", "apply_failed", "database apply failed", map[string]any{"cycle": 1, "error": err.Error()})
		log.Printf("saving migrations to %s...", r.cfg.Runtime.SQLOutputPath)
		if len(sqlBytes) > 0 {
			if writeErr := r.writeFile(r.cfg.Runtime.SQLOutputPath, sqlBytes, 0644); writeErr != nil {
				return r.failRun(1, fmt.Errorf("failed to write migrations to file: %w", writeErr))
			}
			fmt.Fprintf(r.out, "Migrations saved to %s\n", r.cfg.Runtime.SQLOutputPath)
		}
		r.finishRun(1, RunOutcomeFailed, metrics, 0, err, false)
		return nil
	}

	fmt.Fprintf(r.out, "Applied %d migrations\n", result.AppliedCount)
	r.finishRun(1, RunOutcomeSucceeded, metrics, result.AppliedCount, nil, true)
	return nil
}

func (r *Runner) runPeriodicCycle(ctx context.Context, cycle int) error {
	plan, metrics, err := r.buildFullSyncPlan(ctx, cycle)
	if err != nil {
		return err
	}

	sqlBytes, err := r.target.ExportFullSyncSQL(plan)
	if err != nil {
		return r.failRun(cycle, err)
	}

	r.updateApplyMetrics(cycle, metrics)
	result, err := r.target.ApplyFullSync(ctx, plan)
	if err != nil {
		log.Printf("periodic full reload cycle %d failed: %v", cycle, err)
		r.logEvent("error", "full_reload_failed", "periodic full reload cycle failed", map[string]any{"cycle": cycle, "error": err.Error()})
		if len(sqlBytes) > 0 {
			log.Printf("saving full sync SQL to %s...", r.cfg.Runtime.SQLOutputPath)
			if writeErr := r.writeFile(r.cfg.Runtime.SQLOutputPath, sqlBytes, 0644); writeErr != nil {
				return r.failRun(cycle, fmt.Errorf("failed to write migrations to file: %w", writeErr))
			}
			fmt.Fprintf(r.out, "Migrations saved to %s\n", r.cfg.Runtime.SQLOutputPath)
		}
		r.finishRun(cycle, RunOutcomeFailed, metrics, 0, err, false)
		return nil
	}

	fmt.Fprintf(r.out, "Full reload cycle %d applied %d statements\n", cycle, result.AppliedCount)
	r.finishRun(cycle, RunOutcomeSucceeded, metrics, result.AppliedCount, nil, true)
	return nil
}

type runMetrics struct {
	managedTableCount int
	plannedRowCount   int
}

func (r *Runner) buildOneShotPlan(ctx context.Context, cycle int) (*core.MigrationPlan, runMetrics, error) {
	r.startRun(cycle)
	plan, err := r.planner.GeneratePlan(ctx, r.spec, r.cfg.API.BaseURL)
	if err != nil {
		return nil, runMetrics{}, r.failRun(cycle, fmt.Errorf("generate migration plan: %w", err))
	}
	metrics := summarizeMigrationPlan(plan)
	r.withState(func(state *syncState) {
		state.status.ManagedTableCount = metrics.managedTableCount
		state.status.PlannedRowCount = metrics.plannedRowCount
	})
	return plan, metrics, nil
}

func (r *Runner) buildFullSyncPlan(ctx context.Context, cycle int) (*core.FullSyncPlan, runMetrics, error) {
	r.startRun(cycle)
	plan, err := r.planner.GenerateFullSyncPlan(ctx, r.spec, r.cfg.API.BaseURL)
	if err != nil {
		return nil, runMetrics{}, r.failRun(cycle, fmt.Errorf("generate full sync plan: %w", err))
	}
	metrics := summarizeFullSyncPlan(plan)
	r.withState(func(state *syncState) {
		state.status.ManagedTableCount = metrics.managedTableCount
		state.status.PlannedRowCount = metrics.plannedRowCount
	})
	return plan, metrics, nil
}

func (r *Runner) startRun(cycle int) {
	now := time.Now()
	r.tracker.Start()
	r.withState(func(state *syncState) {
		state.status.Cycle = cycle
		state.status.Phase = PhaseRunningFetch
		state.status.StartedAt = &now
		state.status.FinishedAt = nil
		state.status.NextRunAt = nil
		state.status.RequestCount = 0
		state.status.FailedRequestCount = 0
		state.status.ManagedTableCount = 0
		state.status.PlannedRowCount = 0
		state.status.AppliedStatementCount = 0
		state.status.LastError = ""
	})
	r.logEvent("info", "cycle_started", "cycle started", map[string]any{"cycle": cycle})
}

func (r *Runner) updateApplyMetrics(cycle int, metrics runMetrics) {
	r.withState(func(state *syncState) {
		if state.status.Cycle != cycle {
			return
		}
		state.status.Phase = PhaseRunningApply
		state.status.ManagedTableCount = metrics.managedTableCount
		state.status.PlannedRowCount = metrics.plannedRowCount
	})
}

func (r *Runner) finishRun(cycle int, outcome RunOutcome, metrics runMetrics, appliedCount int, err error, markSuccess bool) {
	finishedAt := time.Now()
	requestCount, failedRequestCount := r.tracker.Snapshot()
	r.tracker.Finish()

	r.withState(func(state *syncState) {
		state.status.Cycle = cycle
		state.status.FinishedAt = &finishedAt
		state.status.RequestCount = requestCount
		state.status.FailedRequestCount = failedRequestCount
		state.status.ManagedTableCount = metrics.managedTableCount
		state.status.PlannedRowCount = metrics.plannedRowCount
		state.status.AppliedStatementCount = appliedCount
		if err != nil {
			state.status.LastError = err.Error()
		}
		if markSuccess {
			state.status.LastError = ""
			state.status.LastSuccessAt = &finishedAt
		}
		if !r.cfg.Runtime.FullReloadEnabled {
			if outcome == RunOutcomeSucceeded {
				state.status.Phase = PhaseCompleted
			} else {
				state.status.Phase = PhaseFailed
			}
		}

		run := RunSummary{
			Status:  cloneStatus(state.status),
			Outcome: outcome,
		}
		state.runHistory = append([]RunSummary{run}, state.runHistory...)
		if len(state.runHistory) > state.limit {
			state.runHistory = state.runHistory[:state.limit]
		}
	})

	level := "info"
	kind := "cycle_completed"
	message := "cycle completed"
	if outcome == RunOutcomeFailed {
		level = "error"
		kind = "cycle_failed"
		message = "cycle failed"
	}
	r.logEvent(level, kind, message, map[string]any{
		"cycle":                cycle,
		"outcome":              outcome,
		"request_count":        requestCount,
		"failed_request_count": failedRequestCount,
		"managed_table_count":  metrics.managedTableCount,
		"planned_row_count":    metrics.plannedRowCount,
		"applied_count":        appliedCount,
	})
}

func (r *Runner) failRun(cycle int, err error) error {
	finishedAt := time.Now()
	requestCount, failedRequestCount := r.tracker.Snapshot()
	r.tracker.Finish()
	r.withState(func(state *syncState) {
		state.status.Cycle = cycle
		state.status.Phase = PhaseFailed
		state.status.FinishedAt = &finishedAt
		state.status.RequestCount = requestCount
		state.status.FailedRequestCount = failedRequestCount
		state.status.LastError = err.Error()
		run := RunSummary{
			Status:  cloneStatus(state.status),
			Outcome: RunOutcomeFailed,
		}
		state.runHistory = append([]RunSummary{run}, state.runHistory...)
		if len(state.runHistory) > state.limit {
			state.runHistory = state.runHistory[:state.limit]
		}
	})
	r.logEvent("error", "runner_failed", "runner failed", map[string]any{
		"cycle":                cycle,
		"error":                err.Error(),
		"request_count":        requestCount,
		"failed_request_count": failedRequestCount,
	})
	return err
}

func (r *Runner) markFailure(err error) {
	r.withState(func(state *syncState) {
		state.status.Phase = PhaseFailed
		state.status.LastError = err.Error()
	})
}

func (r *Runner) setStopping() {
	r.withState(func(state *syncState) {
		state.status.Phase = PhaseStopping
		state.status.NextRunAt = nil
	})
	r.logEvent("info", "runner_stopping", "runner stopping", nil)
}

func (r *Runner) logEvent(level, kind, message string, fields map[string]any) {
	if r.eventLogger == nil {
		return
	}
	r.eventLogger.LogEvent(observability.Event{
		Timestamp: time.Now().UTC(),
		Level:     level,
		Kind:      kind,
		Message:   message,
		Fields:    fields,
	})
}

func (r *Runner) withState(fn func(state *syncState)) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	fn(r.state)
}

func summarizeMigrationPlan(plan *core.MigrationPlan) runMetrics {
	metrics := runMetrics{}
	if plan == nil {
		return metrics
	}
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case core.CreateTableOp:
			metrics.managedTableCount++
		case *core.CreateTableOp:
			metrics.managedTableCount++
		case core.CreateLinkTableOp:
			metrics.managedTableCount++
		case *core.CreateLinkTableOp:
			metrics.managedTableCount++
		case core.InsertRowsOp:
			metrics.plannedRowCount += len(typed.Rows)
		case *core.InsertRowsOp:
			metrics.plannedRowCount += len(typed.Rows)
		}
	}
	return metrics
}

func summarizeFullSyncPlan(plan *core.FullSyncPlan) runMetrics {
	metrics := runMetrics{}
	if plan == nil {
		return metrics
	}
	metrics.managedTableCount = len(plan.Tables)
	for _, table := range plan.Tables {
		metrics.plannedRowCount += len(table.Rows)
	}
	return metrics
}

func cloneStatus(status Status) Status {
	return Status{
		JobMode:               status.JobMode,
		Phase:                 status.Phase,
		Cycle:                 status.Cycle,
		StartedAt:             cloneTime(status.StartedAt),
		FinishedAt:            cloneTime(status.FinishedAt),
		NextRunAt:             cloneTime(status.NextRunAt),
		RequestCount:          status.RequestCount,
		FailedRequestCount:    status.FailedRequestCount,
		ManagedTableCount:     status.ManagedTableCount,
		PlannedRowCount:       status.PlannedRowCount,
		AppliedStatementCount: status.AppliedStatementCount,
		LastError:             status.LastError,
		LastSuccessAt:         cloneTime(status.LastSuccessAt),
	}
}

func cloneTime(ts *time.Time) *time.Time {
	if ts == nil {
		return nil
	}
	value := *ts
	return &value
}
