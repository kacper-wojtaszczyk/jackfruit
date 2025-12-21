package cds

import "fmt"

// apiError represents an error from the CDS API.
type apiError struct {
	StatusCode int
	Message    string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("cds: %s (status %d)", e.Message, e.StatusCode)
}

type ClientError struct {
	Message string
}

func (e *ClientError) Error() string {
	return fmt.Sprintf("cds client: %s", e.Message)
}
