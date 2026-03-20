package api_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/api"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/grid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/lineage"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/testutil"
)

func setupStack(t *testing.T) (*http.ServeMux, *sql.DB) {
	t.Helper()

	pgDB := testutil.NewPostgresDB(t)
	chConn := testutil.NewRawConn(t)
	service := domain.NewService(grid.NewFinder(chConn), lineage.NewFinder(pgDB))
	logger := slog.New(slog.DiscardHandler)
	mux := http.NewServeMux()
	api.NewHandler(service, logger).RegisterRoutes(mux)

	return mux, pgDB
}

func TestHealthHandler(t *testing.T) {
	mux := http.NewServeMux()
	api.NewHandler(nil, nil).RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", w.Code)
	}
	if body := w.Body.String(); body != "" {
		t.Errorf("expected empty body, got %q", body)
	}
}

func TestEnvironmentalHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test, requires infrastructure running")
	}

	mux, pgDB := setupStack(t)
	rawConn := testutil.NewRawConn(t)

	t.Run("single variable", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second)
		lat := float32(52.5)
		lon := float32(13.4)

		catalogID := testutil.InsertGridRow(t, rawConn, "pm2p5", float32(12.5), "µg/m³", ts, lat, lon)
		rawFileID := testutil.SeedLineage(t, pgDB, catalogID, "ads", "cams-europe-air-quality-forecast", "pm2p5", "µg/m³")

		url := fmt.Sprintf("/v1/environmental?lat=%v&lon=%v&timestamp=%s&variables=pm2p5",
			lat, lon, ts.Format(time.RFC3339))
		req := httptest.NewRequest("GET", url, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.EnvironmentalResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}

		if len(resp.Variables) != 1 {
			t.Fatalf("expected 1 variable, got %d", len(resp.Variables))
		}
		v := resp.Variables[0]
		if v.Name != "pm2p5" {
			t.Errorf("expected name pm2p5, got %q", v.Name)
		}
		if float32(v.Value) != float32(12.5) {
			t.Errorf("expected value 12.5, got %v", v.Value)
		}
		if v.Unit != "µg/m³" {
			t.Errorf("expected unit µg/m³, got %q", v.Unit)
		}
		if v.ActualLat != lat {
			t.Errorf("expected actual_lat %v, got %v", lat, v.ActualLat)
		}
		if v.ActualLon != lon {
			t.Errorf("expected actual_lon %v, got %v", lon, v.ActualLon)
		}
		if !v.RefTimestamp.Equal(ts) {
			t.Errorf("expected ref_timestamp %v, got %v", ts, v.RefTimestamp)
		}
		assertLineage(t, v.Lineage, "ads", "cams-europe-air-quality-forecast", rawFileID)
	})

	t.Run("multiple variables", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second)
		lat := float32(48.1)
		lon := float32(11.6)

		catID1 := testutil.InsertGridRow(t, rawConn, "pm2p5_multi", float32(8.1), "µg/m³", ts, lat, lon)
		catID2 := testutil.InsertGridRow(t, rawConn, "no2_multi", float32(20.3), "µg/m³", ts, lat, lon)
		rawFileID1 := testutil.SeedLineage(t, pgDB, catID1, "ads", "cams-europe-air-quality-forecast", "pm2p5_multi", "µg/m³")
		rawFileID2 := testutil.SeedLineage(t, pgDB, catID2, "ads", "cams-europe-air-quality-forecast", "no2_multi", "µg/m³")

		url := fmt.Sprintf("/v1/environmental?lat=%v&lon=%v&timestamp=%s&variables=pm2p5_multi,no2_multi",
			lat, lon, ts.Format(time.RFC3339))
		req := httptest.NewRequest("GET", url, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.EnvironmentalResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}

		if len(resp.Variables) != 2 {
			t.Fatalf("expected 2 variables, got %d", len(resp.Variables))
		}

		byName := make(map[string]api.VariableResponse)
		for _, v := range resp.Variables {
			byName[v.Name] = v
		}

		if v, ok := byName["pm2p5_multi"]; !ok {
			t.Error("expected pm2p5_multi in response")
		} else {
			if float32(v.Value) != float32(8.1) {
				t.Errorf("expected pm2p5_multi value 8.1, got %v", v.Value)
			}
			assertLineage(t, v.Lineage, "ads", "cams-europe-air-quality-forecast", rawFileID1)
		}

		if v, ok := byName["no2_multi"]; !ok {
			t.Error("expected no2_multi in response")
		} else {
			if float32(v.Value) != float32(20.3) {
				t.Errorf("expected no2_multi value 20.3, got %v", v.Value)
			}
			assertLineage(t, v.Lineage, "ads", "cams-europe-air-quality-forecast", rawFileID2)
		}
	})

	t.Run("variable not found", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second)
		url := fmt.Sprintf("/v1/environmental?lat=52.5&lon=13.4&timestamp=%s&variables=nonexistent_var",
			ts.Format(time.RFC3339))
		req := httptest.NewRequest("GET", url, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var errResp api.ErrorResponse
		if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
			t.Fatalf("decode error response: %v", err)
		}
		if !strings.Contains(errResp.Error, "nonexistent_var") {
			t.Errorf("expected error to mention variable name, got %q", errResp.Error)
		}
	})

	t.Run("nearest neighbor returns actual grid coords", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second)
		gridLat := float32(52.5)
		gridLon := float32(13.4)

		catalogID := testutil.InsertGridRow(t, rawConn, "pm2p5_nn", float32(9.9), "µg/m³", ts, gridLat, gridLon)
		rawFileID := testutil.SeedLineage(t, pgDB, catalogID, "ads", "cams-europe-air-quality-forecast", "pm2p5_nn", "µg/m³")

		// Request at slightly different coords — should snap to the nearest grid point.
		reqLat := float32(52.51)
		reqLon := float32(13.41)
		url := fmt.Sprintf("/v1/environmental?lat=%v&lon=%v&timestamp=%s&variables=pm2p5_nn",
			reqLat, reqLon, ts.Format(time.RFC3339))
		req := httptest.NewRequest("GET", url, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.EnvironmentalResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}

		// Top-level lat/lon must echo the request coordinates.
		if resp.Lat != reqLat {
			t.Errorf("resp.Lat = %v, want request lat %v", resp.Lat, reqLat)
		}
		if resp.Lon != reqLon {
			t.Errorf("resp.Lon = %v, want request lon %v", resp.Lon, reqLon)
		}

		if len(resp.Variables) != 1 {
			t.Fatalf("expected 1 variable, got %d", len(resp.Variables))
		}
		v := resp.Variables[0]

		// actual_lat/actual_lon must reflect the nearest grid point, not the request.
		if v.ActualLat != gridLat {
			t.Errorf("v.ActualLat = %v, want grid lat %v", v.ActualLat, gridLat)
		}
		if v.ActualLon != gridLon {
			t.Errorf("v.ActualLon = %v, want grid lon %v", v.ActualLon, gridLon)
		}
		assertLineage(t, v.Lineage, "ads", "cams-europe-air-quality-forecast", rawFileID)
	})

	t.Run("bad request - missing lat", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second)
		url := fmt.Sprintf("/v1/environmental?lon=13.4&timestamp=%s&variables=pm2p5",
			ts.Format(time.RFC3339))
		req := httptest.NewRequest("GET", url, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("bad request - invalid timestamp", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/environmental?lat=52.5&lon=13.4&timestamp=notadate&variables=pm2p5", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("bad request - empty variables", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second)
		url := fmt.Sprintf("/v1/environmental?lat=52.5&lon=13.4&timestamp=%s",
			ts.Format(time.RFC3339))
		req := httptest.NewRequest("GET", url, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})
}

func assertLineage(t *testing.T, l api.LineageResponse, source, dataset string, rawFileID uuid.UUID) {
	t.Helper()
	if l.Source != source {
		t.Errorf("expected lineage source %q, got %q", source, l.Source)
	}
	if l.Dataset != dataset {
		t.Errorf("expected lineage dataset %q, got %q", dataset, l.Dataset)
	}
	if l.RawFileID != rawFileID {
		t.Errorf("expected lineage raw_file_id %v, got %v", rawFileID, l.RawFileID)
	}
}
