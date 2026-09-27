// Package agent manages agent lifecycle, sessions, and LLM prompting.
package agent

// StreamEventType identifies the kind of event in a streaming response.
type StreamEventType string

// StreamEventType values.
const (
	StreamEventText         StreamEventType = "text"          // partial text from the LLM
	StreamEventTool         StreamEventType = "tool"          // tool execution metadata
	StreamEventToolProgress StreamEventType = "tool_progress" // safe public tool state
	StreamEventMedia        StreamEventType = "media"         // image data URL from the LLM
	StreamEventDone         StreamEventType = "done"          // stream complete
	StreamEventError        StreamEventType = "error"         // error occurred
	StreamEventStop         StreamEventType = "stop"          // agent was stopped mid-stream
)

// ToolState is the explicit lifecycle state of a tool invocation.
type ToolState string

const (
	// ToolStateStarted means the registered invocation is about to run.
	ToolStateStarted ToolState = "started"
	// ToolStateSucceeded means the invocation returned without an error, even if its result is empty.
	ToolStateSucceeded ToolState = "succeeded"
	// ToolStateFailed means the invocation returned an error.
	ToolStateFailed ToolState = "failed"
)

// PublicToolEvent contains only fields safe to publish in channel progress.
type PublicToolEvent struct {
	Name         string
	InvocationID string
	State        ToolState
}

// ToolEvent carries structured tool execution details for debug-oriented UIs.
type ToolEvent struct {
	Name         string
	InvocationID string
	State        ToolState
	Args         map[string]any
	Result       string
	Error        string
}

// StreamEvent is a single event emitted during an agent response.
type StreamEvent struct {
	Private         bool // tool details and progress are private and must not be published to shared channels
	Type            StreamEventType
	AgentID         string
	Text            string // set for StreamEventText
	Model           string // model that completed the run; set for StreamEventDone
	Tool            *ToolEvent
	PublicTool      *PublicToolEvent // set only for non-private registered tool calls
	AlreadyAnswered bool             // StreamEventDone is a no-op for an already answered prompt
	MediaURL        string           // set for StreamEventMedia (image data URL or remote URL)
	Err             error            // set for StreamEventError
	StopCause       StopCause        // set for StreamEventStop
}

// StreamConsumer receives StreamEvents.
type StreamConsumer func(StreamEvent)
