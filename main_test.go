package main

import "testing"

func TestPrepareStatement(t *testing.T) {
	tests := []struct {
		line       string
		wantResult PrepareResult
		wantStmt   StatementType
	}{
		{
			line:       "select",
			wantResult: PREPARE_SUCCESS,
			wantStmt:   STATEMENT_SELECT,
		},
		{
			line:       "select * from",
			wantResult: PREPARE_SUCCESS,
			wantStmt:   STATEMENT_SELECT,
		},
		{
			line:       "se",
			wantResult: PREPARE_UNRECOGNIZED_STATEMENT,
		},
		{
			line:       "selec",
			wantResult: PREPARE_UNRECOGNIZED_STATEMENT,
		},
	}

	for _, tt := range tests {
		stmt := &Statement{}

		result := prepareStatement(tt.line, stmt)

		if result != tt.wantResult {
			t.Fatalf("unexpected result. want: %d, got: %d", tt.wantResult, result)
		}

		if stmt.Type != tt.wantStmt {
			t.Fatalf("unexpected result. want: %d, got: %d", tt.wantStmt, stmt.Type)
		}
	}
}
