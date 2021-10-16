package main

import (
	"reflect"
	"testing"
)

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

		result := PrepareStatement(tt.line, stmt)

		if result != tt.wantResult {
			t.Fatalf("unexpected result. want: %d, got: %d", tt.wantResult, result)
		}

		if stmt.Type != tt.wantStmt {
			t.Fatalf("unexpected result. want: %d, got: %d", tt.wantStmt, stmt.Type)
		}
	}
}

func TestSerializeAndDeserializeRow(t *testing.T) {
	row := Row{
		ID:       42,
		UserName: [COLUMN_USERNAME_SIZE]byte{65},
		Email:    [COLUMN_EMAIL_SIZE]byte{66},
	}

	var page Page
	var offset uint32

	SerializeRow(row, &page, offset)

	var got Row
	DeserializeRow(&page, offset, &got)

	if !reflect.DeepEqual(row, got) {
		t.Fatalf("unexpected. got: %s, want: %s\n", got, row)
	}
}
