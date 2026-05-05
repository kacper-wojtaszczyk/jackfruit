package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Compile-time guarantees that both wrappers expose the underlying
// ResponseWriter via Unwrap. http.ResponseController and similar
// helpers walk Unwrap chains; removing these would silently break
// flushers, hijackers, etc.
var _ interface{ Unwrap() http.ResponseWriter } = (*recoverWriter)(nil)
var _ interface{ Unwrap() http.ResponseWriter } = (*statusRecorder)(nil)

// capturingHandler is an slog.Handler that retains every record so
// tests can assert on log fields. Logging is part of the JF-02
// contract (panic-recovery log, /health skip, client="unknown" for
// missing UA), so log assertions here are intentional, not
// implementation-coupling.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *capturingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, len(h.records))
	copy(out, h.records)
	return out
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func capturingLogger() (*slog.Logger, *capturingHandler) {
	h := &capturingHandler{}
	return slog.New(h), h
}

func attrValue(t *testing.T, r slog.Record, key string) (slog.Value, bool) {
	t.Helper()
	var found slog.Value
	var ok bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = a.Value
			ok = true
			return false
		}
		return true
	})
	return found, ok
}

func requireAttr(t *testing.T, r slog.Record, key string) slog.Value {
	t.Helper()
	v, ok := attrValue(t, r, key)
	if !ok {
		t.Fatalf("log record %q missing attr %q", r.Message, key)
	}
	return v
}

// ---- Recovery middleware ---------------------------------------------------

func TestRecoveryMiddleware_PanicReturns500(t *testing.T) {
	logger, captured := capturingLogger()
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("kaboom")
	})
	wrapped := RecoveryMiddleware(logger)(panicHandler)

	req := httptest.NewRequest(http.MethodGet, "/explode", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}

	body := w.Body.String()
	var errResp ErrorResponse
	if err := json.Unmarshal([]byte(body), &errResp); err != nil {
		t.Fatalf("decode body: %v (body=%q)", err, body)
	}
	if errResp.Error != "internal server error" {
		t.Errorf("error message = %q, want %q", errResp.Error, "internal server error")
	}
	if strings.Contains(body, "kaboom") {
		t.Errorf("panic value leaked into response body: %q", body)
	}

	records := captured.snapshot()
	if len(records) != 1 {
		t.Fatalf("captured %d records, want 1", len(records))
	}
	rec := records[0]
	if rec.Level != slog.LevelError {
		t.Errorf("log level = %s, want ERROR", rec.Level)
	}
	if rec.Message != "panic recovered" {
		t.Errorf("log message = %q, want %q", rec.Message, "panic recovered")
	}
	if v := requireAttr(t, rec, "method"); v.String() != http.MethodGet {
		t.Errorf("method attr = %q, want %q", v.String(), http.MethodGet)
	}
	if v := requireAttr(t, rec, "path"); !strings.Contains(v.String(), "/explode") {
		t.Errorf("path attr = %q, want to contain %q", v.String(), "/explode")
	}
	if v := requireAttr(t, rec, "stack"); v.String() == "" {
		t.Error("stack attr is empty")
	}
	if _, ok := attrValue(t, rec, "error"); !ok {
		t.Error("error attr missing from panic-recovered log")
	}
}

func TestRecoveryMiddleware_NoPanicPassthrough(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("ok"))
	})
	wrapped := RecoveryMiddleware(discardLogger())(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", w.Code)
	}
	if w.Body.String() != "ok" {
		t.Errorf("body = %q, want %q", w.Body.String(), "ok")
	}
}

func TestRecoveryMiddleware_PanicAfterPartialWrite(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("partial body"))
		panic("mid-write kaboom")
	})
	wrapped := RecoveryMiddleware(discardLogger())(handler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	// Headers were already sent before the panic — recovery must not
	// attempt a second WriteHeader and must not append the error JSON.
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (already sent before panic)", w.Code)
	}
	if got := w.Body.String(); got != "partial body" {
		t.Errorf("body = %q, want exactly %q", got, "partial body")
	}
	if strings.Contains(w.Body.String(), "internal server error") {
		t.Error("recovery wrote error JSON on top of an already-written response")
	}
}

func TestRecoverWriter_WriteSetsWrittenFlag(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &recoverWriter{ResponseWriter: rec}

	if rw.written {
		t.Fatal("written should be false before any write")
	}
	if _, err := rw.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !rw.written {
		t.Error("written should be true after Write")
	}
}

func TestRecoverWriter_WriteHeaderSetsWrittenFlag(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &recoverWriter{ResponseWriter: rec}

	rw.WriteHeader(http.StatusTeapot)

	if !rw.written {
		t.Error("written should be true after WriteHeader")
	}
	if rec.Code != http.StatusTeapot {
		t.Errorf("underlying status = %d, want 418", rec.Code)
	}
}

// ---- Status recorder -------------------------------------------------------

func TestStatusRecorder_CapturesExplicitStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec}

	sr.WriteHeader(http.StatusNotFound)

	if sr.status != http.StatusNotFound {
		t.Errorf("recorder.status = %d, want 404", sr.status)
	}
	if rec.Code != http.StatusNotFound {
		t.Errorf("underlying status = %d, want 404", rec.Code)
	}
}

func TestStatusRecorder_WriteWithoutHeaderDefaultsTo200(t *testing.T) {
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec}

	if _, err := sr.Write([]byte("hi")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if sr.status != http.StatusOK {
		t.Errorf("recorder.status = %d, want 200 (defaulted on bare Write)", sr.status)
	}
}

// ---- Logging middleware ----------------------------------------------------

func TestLoggingMiddleware_LogsRequiredFields(t *testing.T) {
	logger, captured := capturingLogger()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("created"))
	})
	wrapped := LoggingMiddleware(logger)(handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/environmental?lat=52.5", nil)
	req.Header.Set("User-Agent", "buttprint-api/0.1.0")
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", w.Code)
	}
	if w.Body.String() != "created" {
		t.Errorf("body = %q, want %q", w.Body.String(), "created")
	}

	records := captured.snapshot()
	if len(records) != 1 {
		t.Fatalf("captured %d log records, want 1", len(records))
	}
	rec := records[0]
	if rec.Message != "request" {
		t.Errorf("log message = %q, want %q", rec.Message, "request")
	}
	if rec.Level != slog.LevelInfo {
		t.Errorf("log level = %s, want INFO", rec.Level)
	}

	checks := map[string]func(slog.Value) error{
		"method":   stringEquals(http.MethodGet),
		"path":     stringContains("/v1/environmental"),
		"status":   int64Equals(int64(http.StatusCreated)),
		"client":   stringEquals("buttprint-api/0.1.0"),
		"duration": durationNonNegative(),
	}
	for key, check := range checks {
		v, ok := attrValue(t, rec, key)
		if !ok {
			t.Errorf("log record missing attr %q", key)
			continue
		}
		if err := check(v); err != nil {
			t.Errorf("attr %q: %v", key, err)
		}
	}
}

func TestLoggingMiddleware_SkipsHealth(t *testing.T) {
	logger, captured := capturingLogger()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	wrapped := LoggingMiddleware(logger)(handler)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
	if recs := captured.snapshot(); len(recs) != 0 {
		t.Errorf("captured %d log records for /health, want 0", len(recs))
	}
}

func TestLoggingMiddleware_StatusDefaultsTo200(t *testing.T) {
	logger, captured := capturingLogger()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No WriteHeader — status should be inferred as 200.
		w.Write([]byte("hi"))
	})
	wrapped := LoggingMiddleware(logger)(handler)

	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	wrapped.ServeHTTP(httptest.NewRecorder(), req)

	records := captured.snapshot()
	if len(records) != 1 {
		t.Fatalf("captured %d records, want 1", len(records))
	}
	v := requireAttr(t, records[0], "status")
	if v.Int64() != int64(http.StatusOK) {
		t.Errorf("logged status = %d, want 200", v.Int64())
	}
}

func TestLoggingMiddleware_MissingUserAgentLogsUnknown(t *testing.T) {
	logger, captured := capturingLogger()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := LoggingMiddleware(logger)(handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/environmental", nil)
	// Defensively clear the header in case future stdlib versions
	// start populating it.
	req.Header.Del("User-Agent")
	wrapped.ServeHTTP(httptest.NewRecorder(), req)

	records := captured.snapshot()
	if len(records) != 1 {
		t.Fatalf("captured %d records, want 1", len(records))
	}
	v := requireAttr(t, records[0], "client")
	if v.String() != "unknown" {
		t.Errorf("client = %q, want %q", v.String(), "unknown")
	}
}

// ---- Composite stack -------------------------------------------------------

func TestRecoveryAndLogging_StackSurvivesPanic(t *testing.T) {
	logger, captured := capturingLogger()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Same wiring order as cmd/serving/main.go: recovery outermost.
	var h http.Handler = mux
	h = LoggingMiddleware(logger)(h)
	h = RecoveryMiddleware(logger)(h)

	// First request: panic. Stack must catch it.
	{
		req := httptest.NewRequest(http.MethodGet, "/panic", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("panic request status = %d, want 500", w.Code)
		}
		var errResp ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
			t.Fatalf("decode error response: %v (body=%q)", err, w.Body.String())
		}
		if errResp.Error != "internal server error" {
			t.Errorf("error message = %q, want %q", errResp.Error, "internal server error")
		}
	}

	// Second request: must still serve normally on the same chain.
	{
		req := httptest.NewRequest(http.MethodGet, "/ok", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("post-panic request status = %d, want 200", w.Code)
		}
		if w.Body.String() != "ok" {
			t.Errorf("post-panic body = %q, want %q", w.Body.String(), "ok")
		}
	}

	// Sanity-check that we observed both the panic-recovery log and a
	// normal request log for the second call.
	var sawPanicLog, sawRequestLog bool
	for _, rec := range captured.snapshot() {
		switch rec.Message {
		case "panic recovered":
			sawPanicLog = true
		case "request":
			if v, ok := attrValue(t, rec, "status"); ok && v.Int64() == int64(http.StatusOK) {
				sawRequestLog = true
			}
		}
	}
	if !sawPanicLog {
		t.Error("expected a 'panic recovered' log record")
	}
	if !sawRequestLog {
		t.Error("expected a 'request' log record with status=200 for /ok")
	}
}

// ---- attr-value helpers ----------------------------------------------------

func stringEquals(want string) func(slog.Value) error {
	return func(v slog.Value) error {
		if v.String() != want {
			return fmt.Errorf("got %q, want %q", v.String(), want)
		}
		return nil
	}
}

func stringContains(sub string) func(slog.Value) error {
	return func(v slog.Value) error {
		if !strings.Contains(v.String(), sub) {
			return fmt.Errorf("got %q, want to contain %q", v.String(), sub)
		}
		return nil
	}
}

func int64Equals(want int64) func(slog.Value) error {
	return func(v slog.Value) error {
		if v.Int64() != want {
			return fmt.Errorf("got %d, want %d", v.Int64(), want)
		}
		return nil
	}
}

func durationNonNegative() func(slog.Value) error {
	return func(v slog.Value) error {
		if v.Kind() != slog.KindDuration {
			return fmt.Errorf("kind = %s, want Duration", v.Kind())
		}
		if v.Duration() < 0 {
			return fmt.Errorf("duration %s is negative", v.Duration())
		}
		return nil
	}
}
