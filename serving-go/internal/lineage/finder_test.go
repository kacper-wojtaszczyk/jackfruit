package lineage

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

func TestGetLineage_Found(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	rawFileID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	rows := sqlmock.NewRows([]string{"source", "dataset", "raw_file_id"}).
		AddRow("ads", "cams-europe-air-quality-forecast", rawFileID)
	mock.ExpectQuery("SELECT rf\\.source, rf\\.dataset, cd\\.raw_file_id").
		WithArgs(catalogID).
		WillReturnRows(rows)

	finder := NewFinder(db)
	lineage, err := finder.GetLineage(t.Context(), catalogID)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if lineage.Source != "ads" {
		t.Errorf("expected source %q, got %q", "ads", lineage.Source)
	}
	if lineage.Dataset != "cams-europe-air-quality-forecast" {
		t.Errorf("expected dataset %q, got %q", "cams-europe-air-quality-forecast", lineage.Dataset)
	}
	if lineage.RawFileID != rawFileID {
		t.Errorf("expected raw_file_id %v, got %v", rawFileID, lineage.RawFileID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetLineage_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectQuery("SELECT rf\\.source, rf\\.dataset, cd\\.raw_file_id").
		WithArgs(catalogID).
		WillReturnError(sql.ErrNoRows)

	finder := NewFinder(db)
	_, err = finder.GetLineage(t.Context(), catalogID)
	if !errors.Is(err, domain.ErrLineageNotFound) {
		t.Errorf("expected ErrLineageNotFound, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetLineage_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	dbErr := errors.New("connection refused")

	mock.ExpectQuery("SELECT rf\\.source, rf\\.dataset, cd\\.raw_file_id").
		WithArgs(catalogID).
		WillReturnError(dbErr)

	finder := NewFinder(db)
	_, err = finder.GetLineage(t.Context(), catalogID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, domain.ErrLineageNotFound) {
		t.Error("error should not be ErrLineageNotFound for a generic DB error")
	}
	if !errors.Is(err, dbErr) {
		t.Errorf("expected error wrapping original DB error, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
