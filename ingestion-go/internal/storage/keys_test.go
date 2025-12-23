package storage

import (
	"testing"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
)

func TestObjectKey_Key(t *testing.T) {
	key := ObjectKey{
		Source:    "ads",
		Dataset:   model.Dataset("cams-europe-air-quality-forecasts-analysis"),
		Date:      "2025-03-12",
		RunID:     "01890c24-905b-7122-b170-b60814e6ee06",
		Extension: "grib",
	}

	got := key.Key()
	want := "ads/cams-europe-air-quality-forecasts-analysis/2025-03-12/01890c24-905b-7122-b170-b60814e6ee06.grib"

	if got != want {
		t.Fatalf("Key() = %s, want %s", got, want)
	}
}
