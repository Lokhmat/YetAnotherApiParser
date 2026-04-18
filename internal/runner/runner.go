package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"strings"
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

var (
	ErrManualCycleUnsupported  = errors.New("manual cycle trigger is available only in periodic mode")
	ErrCycleTriggerUnavailable = errors.New("cycle can be triggered only while runner is sleeping")
	ErrCycleTriggerPending     = errors.New("manual cycle trigger is already pending")
)

type Planner interface {
	GenerateCyclePlan(ctx context.Context, spec *openapi3.T, baseURL string, checkpoints core.MigrationTarget) (*core.CyclePlan, error)
	CommitCycle(plan *core.CyclePlan)
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
	triggerNext chan struct{}

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
	if cfg.Runtime.CycleEnabled {
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
		triggerNext: make(chan struct{}, 1),
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

	if !r.cfg.Runtime.CycleEnabled {
		err := r.runOneShot(ctx)
		if err != nil {
			r.markFailure(err)
		}
		return err
	}

	fmt.Fprintf(r.out, "Periodic cycles enabled: every %ds\n", r.cfg.Runtime.CycleIntervalSeconds)

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

		nextRunAt := time.Now().Add(r.cfg.Runtime.CycleInterval)
		r.withState(func(state *syncState) {
			state.status.Phase = PhaseSleeping
			state.status.NextRunAt = &nextRunAt
			state.status.FinishedAt = nil
		})
		if err := r.waitForNextCycle(ctx, r.cfg.Runtime.CycleInterval); err != nil {
			if err == context.Canceled {
				r.setStopping()
				return nil
			}
			r.markFailure(err)
			return err
		}
	}
}

func (r *Runner) TriggerCycle() error {
	if !r.cfg.Runtime.CycleEnabled {
		return ErrManualCycleUnsupported
	}

	status := r.StatusSnapshot()
	if status.Phase != PhaseSleeping {
		return ErrCycleTriggerUnavailable
	}

	select {
	case r.triggerNext <- struct{}{}:
		r.withState(func(state *syncState) {
			state.status.NextRunAt = nil
		})
		r.logEvent("info", "cycle_triggered", "manual cycle trigger accepted", map[string]any{"cycle": status.Cycle})
		return nil
	default:
		return ErrCycleTriggerPending
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
	plan, metrics, err := r.buildCyclePlan(ctx, 1)
	if err != nil {
		return err
	}

	sqlBytes, err := r.exportCycleSQL(plan)
	if err != nil {
		return r.failRun(1, err)
	}

	totalOps := 0
	if plan.UpsertPlan != nil {
		totalOps += len(plan.UpsertPlan.Operations)
	}
	if plan.FullSyncPlan != nil {
		totalOps += len(plan.FullSyncPlan.Tables)
	}
	fmt.Fprintf(r.out, "\nGenerated %d operations:\n", totalOps)
	if len(sqlBytes) > 0 {
		fmt.Fprintf(r.out, "\n%s\n", string(sqlBytes))
	}

	r.updateApplyMetrics(1, metrics)
	result, err := r.applyCyclePlan(ctx, plan)
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

	r.planner.CommitCycle(plan)
	fmt.Fprintf(r.out, "Applied %d statements\n", result.AppliedCount)
	r.finishRun(1, RunOutcomeSucceeded, metrics, result.AppliedCount, nil, true)
	return nil
}

func (r *Runner) runPeriodicCycle(ctx context.Context, cycle int) error {
	plan, metrics, err := r.buildCyclePlan(ctx, cycle)
	if err != nil {
		return err
	}

	sqlBytes, err := r.exportCycleSQL(plan)
	if err != nil {
		return r.failRun(cycle, err)
	}

	r.updateApplyMetrics(cycle, metrics)
	result, err := r.applyCyclePlan(ctx, plan)
	if err != nil {
		log.Printf("periodic cycle %d failed: %v", cycle, err)
		r.logEvent("error", "cycle_apply_failed", "periodic cycle failed", map[string]any{"cycle": cycle, "error": err.Error()})
		if len(sqlBytes) > 0 {
			log.Printf("saving cycle SQL to %s...", r.cfg.Runtime.SQLOutputPath)
			if writeErr := r.writeFile(r.cfg.Runtime.SQLOutputPath, sqlBytes, 0644); writeErr != nil {
				return r.failRun(cycle, fmt.Errorf("failed to write migrations to file: %w", writeErr))
			}
			fmt.Fprintf(r.out, "Migrations saved to %s\n", r.cfg.Runtime.SQLOutputPath)
		}
		r.finishRun(cycle, RunOutcomeFailed, metrics, 0, err, false)
		return nil
	}

	r.planner.CommitCycle(plan)
	fmt.Fprintf(r.out, "Cycle %d applied %d statements\n", cycle, result.AppliedCount)
	r.finishRun(cycle, RunOutcomeSucceeded, metrics, result.AppliedCount, nil, true)
	return nil
}

type runMetrics struct {
	managedTableCount int
	plannedRowCount   int
}

func (r *Runner) buildCyclePlan(ctx context.Context, cycle int) (*core.CyclePlan, runMetrics, error) {
	r.startRun(cycle)
	plan, err := r.planner.GenerateCyclePlan(ctx, r.spec, r.cfg.API.BaseURL, r.target)
	if err != nil {
		return nil, runMetrics{}, r.failRun(cycle, fmt.Errorf("generate cycle plan: %w", err))
	}
	if plan != nil && plan.FullSyncPlan != nil && len(plan.FullSyncPlan.Tables) > 0 && !r.target.Capabilities().CanFullSync {
		return nil, runMetrics{}, r.failRun(cycle, fmt.Errorf("db provider %q does not support full-reload operations", r.cfg.Database.Provider))
	}
	metrics := summarizeCyclePlan(plan)
	r.withState(func(state *syncState) {
		state.status.ManagedTableCount = metrics.managedTableCount
		state.status.PlannedRowCount = metrics.plannedRowCount
	})
	return plan, metrics, nil
}

func (r *Runner) startRun(cycle int) {
	now := time.Now()
	r.drainTriggerSignal()
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
		if !r.cfg.Runtime.CycleEnabled {
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

func (r *Runner) waitForNextCycle(ctx context.Context, delay time.Duration) error {
	sleepCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sleepDone := make(chan error, 1)
	go func() {
		sleepDone <- r.sleep(sleepCtx, delay)
	}()

	select {
	case err := <-sleepDone:
		return err
	case <-r.triggerNext:
		cancel()
		select {
		case <-sleepDone:
		default:
		}
		return nil
	}
}

func (r *Runner) drainTriggerSignal() {
	for {
		select {
		case <-r.triggerNext:
		default:
			return
		}
	}
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

func (r *Runner) exportCycleSQL(plan *core.CyclePlan) ([]byte, error) {
	if plan == nil {
		return nil, nil
	}
	chunks := make([]string, 0, 2)
	if plan.UpsertPlan != nil {
		sqlBytes, err := r.target.ExportSQL(plan.UpsertPlan)
		if err != nil {
			return nil, err
		}
		if len(sqlBytes) > 0 {
			chunks = append(chunks, string(sqlBytes))
		}
	}
	if plan.FullSyncPlan != nil {
		sqlBytes, err := r.target.ExportFullSyncSQL(plan.FullSyncPlan)
		if err != nil {
			return nil, err
		}
		if len(sqlBytes) > 0 {
			chunks = append(chunks, string(sqlBytes))
		}
	}
	if len(chunks) == 0 {
		return nil, nil
	}
	return []byte(strings.Join(chunks, "\n\n")), nil
}

func (r *Runner) applyCyclePlan(ctx context.Context, plan *core.CyclePlan) (core.ApplyResult, error) {
	result := core.ApplyResult{}
	if plan == nil {
		return result, nil
	}
	if plan.UpsertPlan != nil {
		applyResult, err := r.target.Apply(ctx, plan.UpsertPlan)
		result.AppliedCount += applyResult.AppliedCount
		if err != nil {
			return result, err
		}
	}
	if plan.FullSyncPlan != nil && len(plan.FullSyncPlan.Tables) > 0 {
		applyResult, err := r.target.ApplyFullSync(ctx, plan.FullSyncPlan)
		result.AppliedCount += applyResult.AppliedCount
		if err != nil {
			return result, err
		}
	}
	if len(plan.PendingCheckpoints) > 0 {
		if err := r.target.SaveCheckpoints(ctx, plan.PendingCheckpoints); err != nil {
			return result, err
		}
	}
	return result, nil
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

func summarizeCyclePlan(plan *core.CyclePlan) runMetrics {
	metrics := runMetrics{}
	if plan == nil {
		return metrics
	}
	upsertMetrics := summarizeMigrationPlan(plan.UpsertPlan)
	fullSyncMetrics := summarizeFullSyncPlan(plan.FullSyncPlan)
	metrics.managedTableCount = upsertMetrics.managedTableCount + fullSyncMetrics.managedTableCount
	metrics.plannedRowCount = upsertMetrics.plannedRowCount + fullSyncMetrics.plannedRowCount
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
