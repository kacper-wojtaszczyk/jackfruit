package api

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

type recoverWriter struct {
	http.ResponseWriter
	written bool
}

func (rw *recoverWriter) WriteHeader(code int) {
	rw.written = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *recoverWriter) Write(b []byte) (int, error) {
	rw.written = true
	return rw.ResponseWriter.Write(b)
}

func (rw *recoverWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

func RecoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &recoverWriter{ResponseWriter: w}
			defer func() {
				if err := recover(); err != nil {
					logger.Error("panic recovered",
						"error", err,
						"stack", string(debug.Stack()),
						"method", r.Method,
						"path", r.URL.Path,
					)
					if !rec.written {
						writeError(w, http.StatusInternalServerError, "internal server error")
					}
					// If headers were already sent, the response is
					// already in-flight — log and let the connection
					// close with a truncated body.
				}
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func LoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &statusRecorder{ResponseWriter: w}

			// Note: if the handler panics, RecoveryMiddleware (outermost)
			// catches it and the log call below never executes. Panicked
			// requests appear only in Recovery's error log, not here.
			next.ServeHTTP(wrapped, r)

			if r.URL.Path == "/health" {
				return
			}

			client := r.UserAgent()
			if client == "" {
				client = "unknown"
			}

			// status stays 0 if the handler returned without writing
			// anything (e.g. client-disconnect path). Logging 0 keeps
			// disconnects distinguishable from real 200s; statusRecorder
			// already defaults to 200 on bare Write([]byte) calls.
			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", wrapped.status,
				"duration", time.Since(start),
				"client", client,
			)
		})
	}
}
