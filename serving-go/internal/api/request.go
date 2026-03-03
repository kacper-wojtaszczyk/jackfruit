package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type EnvironmentalRequest struct {
	Lat       float32
	Lon       float32
	Timestamp time.Time
	Variables []string
}

func ParseEnvironmentalRequest(r *http.Request) (*EnvironmentalRequest, error) {
	query := r.URL.Query()
	lat, err := parseFloat32(query.Get("lat"))
	if err != nil {
		return nil, fmt.Errorf("could not parse latitude: %v", err)
	}
	if lat < -90 || lat > 90 {
		return nil, fmt.Errorf("latitude must be between [-90, 90], %f given", lat)
	}
	lon, err := parseFloat32(query.Get("lon"))
	if err != nil {
		return nil, fmt.Errorf("could not parse longitude: %v", err)
	}
	if lon < -180 || lon > 180 {
		return nil, fmt.Errorf("longitude must be between [-180, 180], %f given", lon)
	}
	timestamp, err := parseTime(query.Get("timestamp"))
	if err != nil {
		return nil, fmt.Errorf("could not parse timestamp: %v", err)
	}
	variables, err := parseStringList(query.Get("variables"))
	if err != nil {
		return nil, fmt.Errorf("could not parse variables: %v", err)
	}
	if len(variables) == 0 {
		return nil, fmt.Errorf("no variables provided")
	}

	return &EnvironmentalRequest{
		Lat:       lat,
		Lon:       lon,
		Timestamp: timestamp,
		Variables: variables,
	}, nil
}

func parseTime(timeString string) (time.Time, error) {
	if timeString == "" {
		return time.Time{}, fmt.Errorf("empty value provided for a required parameter")
	}
	timestamp, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		return time.Time{}, err
	}

	return timestamp, nil
}

func parseFloat32(floatString string) (float32, error) {
	if floatString == "" {
		return 0, fmt.Errorf("empty value provided for a required parameter")
	}
	f, err := strconv.ParseFloat(floatString, 32)
	if err != nil {
		return 0, err
	}

	return float32(f), nil
}

func parseStringList(listString string) ([]string, error) {
	if listString == "" {
		return []string{}, fmt.Errorf("empty value provided for a required parameter")
	}
	vars := strings.Split(listString, ",")
	for i, v := range vars {
		vars[i] = strings.TrimSpace(v)
		if vars[i] == "" {
			return []string{}, fmt.Errorf("empty list element value provided")
		}
	}

	return vars, nil
}
