package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
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
		{
			line:       "",
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

func mkTmpFile(t *testing.T) (string, func()) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("failed to create tmp file: %s", err)
	}
	defer f.Close()
	clean := func() {
		_ = os.Remove(f.Name())
	}
	return f.Name(), clean
}

func TestExecuteStatement(t *testing.T) {
	t.Run("insert tow record", func(t *testing.T) {
		fileName, clean := mkTmpFile(t)
		t.Cleanup(func() {
			clean()
		})
		table := DBOpen(fileName)

		insertRows := []Row{
			{
				ID:       1,
				UserName: UserName{65},
				Email:    Email{66},
			},
			{
				ID:       2,
				UserName: UserName{70},
				Email:    Email{71},
			},
		}

		for i, row := range insertRows {
			stmt := &Statement{
				Type:        STATEMENT_INSERT,
				RowToInsert: row,
			}

			result := ExecuteInsert(stmt, table)
			if result != EXECUTE_SUCCESS {
				t.Fatalf("ExecuteInsert is fail: %d", result)
			}
			stmt = &Statement{
				Type: STATEMENT_SELECT,
			}
			rows, result := ExecuteSelect(stmt, table)
			if result != EXECUTE_SUCCESS {
				t.Fatalf("ExecuteInsert is fail: %d", result)
			}

			if !reflect.DeepEqual(rows[i], row) {
				t.Fatalf("unexpected row. want: %s, got: %s", row, rows[0])
			}
		}
	})

	t.Run("got error message when table is full", func(t *testing.T) {
		fileName, clean := mkTmpFile(t)
		t.Cleanup(func() {
			clean()
		})
		table := DBOpen(fileName)

		insertRow := Row{
			ID:       1,
			UserName: UserName{65},
			Email:    Email{66},
		}
		stmt := &Statement{
			Type:        STATEMENT_INSERT,
			RowToInsert: insertRow,
		}

		for i := 0; i < 1400; i++ {
			result := ExecuteInsert(stmt, table)
			if result != EXECUTE_SUCCESS {
				t.Fatalf("ExecuteInsert is fail: %d", result)
			}
			stmt = &Statement{
				Type: STATEMENT_SELECT,
			}
		}

		result := ExecuteInsert(stmt, table)
		if result != EXECUTE_TABLE_FULL {
			t.Fatalf("result is not EXECUTE_TABLE_FULL: %d", result)
		}

	})
}

func TestScanInput(t *testing.T) {
	t.Run("can insert when strings is minimum length", func(t *testing.T) {
		line := "insert 1 a b"
		row := &Row{}
		err := ScanInput(line, row)
		if err != nil {
			t.Fatalf("scan input got error: %s", err)
		}
	})

	t.Run("can insert when number and strings is maximum length", func(t *testing.T) {
		i64, _ := strconv.ParseUint(strings.Repeat("1", 32), 10, 32)
		id := uint32(i64)
		name := strings.Repeat("a", 32)
		email := strings.Repeat("b", 255)
		line := fmt.Sprintf("insert %d %s %s", id, name, email)

		row := &Row{}
		err := ScanInput(line, row)
		if err != nil {
			t.Fatalf("scan input got error: %s", err)
		}
		if row.ID != id {
			t.Fatalf("unexpected id. want: %d, got: %d", id, row.ID)
		}

		gotName := string(row.UserName[:])
		if gotName != name {
			t.Fatalf("unexpected user name. want: %s, got: %s", name, gotName)
		}

		gotEmail := string(row.Email[:])
		if gotEmail != email {
			t.Fatalf("unexpected email. want: %s, got: %s", email, gotEmail)
		}
	})

	t.Run("cannot insert be cause by invalid syntax", func(t *testing.T) {
		tests := []struct {
			name string
			line string
			want string
		}{
			{
				name: "invalid id",
				line: "insert a b c",
				want: "id is not a number: strconv.ParseUint: parsing \"a\": invalid syntax",
			},
			{
				name: "too long user name",
				line: fmt.Sprintf("insert 1 %s c", strings.Repeat("a", 34)),
				want: "invalid user name: string is too long",
			},
			{
				name: "too long email",
				line: fmt.Sprintf("insert 1 a %s", strings.Repeat("a", 256)),
				want: "invalid email: string is too long",
			},
		}

		for _, tt := range tests {
			err := ScanInput(tt.line, &Row{})
			if err.Error() != tt.want {
				t.Errorf("unexpected error message. want: '%s', got: '%s'", tt.want, err)
			}
		}
	})
}
