package model

import "testing"

func TestRunID_Validate(t *testing.T) {
	tests := []struct {
		name    string
		runID   RunID
		wantErr bool
	}{
		{
			name:    "valid UUIDv7",
			runID:   RunID("01890c24-905b-7122-b170-b60814e6ee06"),
			wantErr: false,
		},
		{
			name:    "empty string",
			runID:   RunID(""),
			wantErr: true,
		},
		{
			name:    "invalid UUID format",
			runID:   RunID("not-a-uuid"),
			wantErr: true,
		},
		{
			name:    "UUIDv4 rejected",
			runID:   RunID("550e8400-e29b-41d4-a716-446655440000"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.runID.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRunID_String(t *testing.T) {
	id := RunID("01890c24-905b-7122-b170-b60814e6ee06")
	if got := id.String(); got != "01890c24-905b-7122-b170-b60814e6ee06" {
		t.Errorf("String() = %v, want %v", got, "01890c24-905b-7122-b170-b60814e6ee06")
	}
}
