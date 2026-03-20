package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/config"
)

func NewPostgresDB(t *testing.T) *sql.DB {
	t.Helper()

	cfg := config.Load()
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.PostgresHost, cfg.PostgresPort, cfg.PostgresUser, cfg.PostgresPassword, cfg.PostgresDB,
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// SeedLineage inserts a raw_files + curated_data pair into Postgres so the
// lineage.Finder can resolve a catalog_id. Returns the generated rawFileID.
func SeedLineage(t *testing.T, db *sql.DB, catalogID uuid.UUID, source, dataset, variable, unit string) uuid.UUID {
	t.Helper()

	rawFileID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	s3Key := fmt.Sprintf("test/%s/%s/%s.grib", source, dataset, catalogID)

	_, err = db.Exec(
		`INSERT INTO catalog.raw_files (id, source, dataset, date, s3_key, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		rawFileID, source, dataset, now.Format("2006-01-02"), s3Key, now,
	)
	if err != nil {
		t.Fatalf("insert raw_files: %v", err)
	}

	_, err = db.Exec(
		`INSERT INTO catalog.curated_data (id, raw_file_id, variable, unit, timestamp, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		catalogID, rawFileID, variable, unit, now, now,
	)
	if err != nil {
		t.Fatalf("insert curated_data: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM catalog.curated_data WHERE id = $1", catalogID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM catalog.raw_files WHERE id = $1", rawFileID)
	})

	return rawFileID
}
