package storage

import (
	"fmt"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
)

type ObjectKey struct {
	Source    string
	Dataset   model.Dataset
	Date      string      // in YYYY-MM-DD format
	RunID     model.RunID // UUIDv7 passed from orchestration
	Extension string
}

func (k ObjectKey) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s.%s", k.Source, k.Dataset, k.Date, k.RunID, k.Extension)
}
