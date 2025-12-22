package main

import "testing"

func TestRun_Skipped(t *testing.T) {
	t.Skip("main-level run testing deferred until e2e path exists")
}
