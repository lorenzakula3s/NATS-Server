package server

import (
	"context"
	"sync"
	"testing"
)

type mockExporter struct {
	mu          sync.Mutex
	spans       []Span
	exportCount int
}

func (m *mockExporter) ExportSpans(ctx context.Context, spans []Span) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = append(m.spans, spans...)
	m.exportCount++
	return nil
}

func TestBatchProcessorConcurrentShutdown(t *testing.T) {
	exporter := &mockExporter{}
	bp := NewBatchSpanProcessor(exporter)

	// Enqueue a set of test spans
	numSpans := 100
	for i := 0; i < numSpans; i++ {
		bp.AddSpan(Span{Name: string(rune(i))})
	}

	// Concurrently invoke Shutdown() and multiple manual Flush() calls
	var wg sync.WaitGroup
	numFlusherGoroutines := 10
	wg.Add(numFlusherGoroutines + 1)

	// Shutdown goroutine
	go func() {
		defer wg.Done()
		_ = bp.Shutdown(context.Background())
	}()

	// Flush goroutines
	for i := 0; i < numFlusherGoroutines; i++ {
		go func() {
			defer wg.Done()
			_ = bp.Flush(context.Background())
		}()
	}

	wg.Wait()

	// Verify that the mock exporter receives each span exactly once
	exporter.mu.Lock()
	defer exporter.mu.Unlock()

	if len(exporter.spans) != numSpans {
		t.Errorf("Expected %d spans, got %d", numSpans, len(exporter.spans))
	}

	// Check for duplicates
	seen := make(map[string]bool)
	for _, span := range exporter.spans {
		if seen[span.Name] {
			t.Errorf("Duplicate span detected: %s", span.Name)
		}
		seen[span.Name] = true
	}
}
