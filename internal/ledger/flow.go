package ledger

import "time"

type FlowStatus string

const (
	FlowRunning   FlowStatus = "RUNNING"
	FlowCompleted FlowStatus = "COMPLETED"
	FlowFailed    FlowStatus = "FAILED"
)

type StepStatus string

const (
	StepCompleted StepStatus = "COMPLETED"
	StepFailed    StepStatus = "FAILED"
)

type FlowStep struct {
	ID        string
	TenantID  string
	FlowRunID string
	StepID    string
	Status    StepStatus
	JournalID string
	ErrorCode string
	CreatedAt time.Time
}

type FlowRun struct {
	ID             string
	TenantID       string
	FlowType       string
	IdempotencyKey string
	SourceService  string
	ActorID        string
	Status         FlowStatus
	Metadata       map[string]any
	CreatedAt      time.Time
	CompletedAt    *time.Time
	FailedAt       *time.Time
	Steps          []FlowStep
}

// StepInput is what callers submit per step.
type StepInput struct {
	StepID  string
	Journal Journal
}
