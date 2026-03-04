package api

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestParseEnvironmentalRequest(t *testing.T) {
	validTime := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)

	tests := []struct {
		name         string
		params       url.Values
		wantErr      bool
		wantVarCount int
	}{
		{
			name: "valid request",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"13.4"},
				"timestamp": {validTime},
				"variables": {"pm2p5"},
			},
			wantErr:      false,
			wantVarCount: 1,
		},
		{
			name: "missing lat",
			params: url.Values{
				"lon":       {"13.4"},
				"timestamp": {validTime},
				"variables": {"pm2p5"},
			},
			wantErr: true,
		},
		{
			name: "missing timestamp",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"13.4"},
				"variables": {"pm2p5"},
			},
			wantErr: true,
		},
		{
			name: "missing variables",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"13.4"},
				"timestamp": {validTime},
			},
			wantErr: true,
		},
		{
			name: "lat out of range",
			params: url.Values{
				"lat":       {"999"},
				"lon":       {"13.4"},
				"timestamp": {validTime},
				"variables": {"pm2p5"},
			},
			wantErr: true,
		},
		{
			name: "lon out of range",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"-999"},
				"timestamp": {validTime},
				"variables": {"pm2p5"},
			},
			wantErr: true,
		},
		{
			name: "invalid timestamp format",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"13.4"},
				"timestamp": {"2025-03-11"},
				"variables": {"pm2p5"},
			},
			wantErr: true,
		},
		{
			name: "multiple vars",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"13.4"},
				"timestamp": {validTime},
				"variables": {"pm2p5,pm10"},
			},
			wantErr:      false,
			wantVarCount: 2,
		},
		{
			name: "empty var in middle",
			params: url.Values{
				"lat":       {"52.5"},
				"lon":       {"13.4"},
				"timestamp": {validTime},
				"variables": {"pm2p5,,pm10"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{URL: &url.URL{RawQuery: tt.params.Encode()}}
			got, err := ParseEnvironmentalRequest(req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseEnvironmentalRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(got.Variables) != tt.wantVarCount {
				t.Errorf("expected %d variables, got %d", tt.wantVarCount, len(got.Variables))
			}
		})
	}
}
