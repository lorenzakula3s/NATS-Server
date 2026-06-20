package server

import (
	"context"
	"sync"
	"sync/atomic"
)

// Span represents a telemetry span.
type Span struct {
	Name string
}

// Exporter defines the interface for exporting spans.
type Exporter interface {
	ExportSpans(ctx context.Context, spans []Span) error
}

// BatchSpanProcessor handles batching and exporting of spans.
type BatchSpanProcessor struct {
	mu       sync.Mutex
	spans    []Span
	exporter Exporter
	stopped  atomic.Bool
}

// NewBatchSpanProcessor creates a new BatchSpanProcessor.
func NewBatchSpanProcessor(exporter Exporter) *BatchSpanProcessor {
	return &BatchSpanProcessor{
		exporter: exporter,
	}
}

// AddSpan adds a span to the batch.
func (b *BatchSpanProcessor) AddSpan(span Span) {
	if b.stopped.Load() {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped.Load() {
		return
	}
	b.spans = append(b.spans, span)
}

// Flush exports any pending spans.
func (b *BatchSpanProcessor) Flush(ctx context.Context) error {
	if b.stopped.Load() {
		return nil
	}
	b.mu.Lock()
	if len(b.spans) == 0 {
		b.mu.Unlock()
		return nil
	}
	spansToFlush := b.spans
	b.spans = nil
	b.mu.Unlock()

	return b.exporter.ExportSpans(ctx, spansToFlush)
}

// Shutdown stops the processor and flushes remaining spans.
func (b *BatchSpanProcessor) Shutdown(ctx context.Context) error {
	if !b.stopped.CompareAndSwap(false, true) {
		return nil // Already shutting down or shut down
	}

	b.mu.Lock()
	spansToFlush := b.spans
	b.spans = nil
	b.mu.Unlock()

	if len(spansToFlush) > 0 {
		return b.exporter.ExportSpans(ctx, spansToFlush)
	}
	return nil
}
