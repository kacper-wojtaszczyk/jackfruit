package storage

import (
	"fmt"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/dataset"
)

type ObjectKey struct {
	Source    string
	Dataset   dataset.Dataset
	Date      string // in YYYY-MM-DD format
	RunID     string // ULID passed from orchestration
	Extension string
}

func (k ObjectKey) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s.%s", k.Source, k.Dataset, k.Date, k.RunID, k.Extension)
}
