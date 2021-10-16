package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/containerd/console"
	"golang.org/x/term"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota + 1
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota + 1
	EXECUTE_TABLE_FULL
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

type Row struct {
	ID       uint32
	UserName [COLUMN_USERNAME_SIZE]byte
	Email    [COLUMN_EMAIL_SIZE]byte
}

func (r Row) Bytes() []byte {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, &r); err != nil {
		log.Println(err)
		return nil
	}

	return buf.Bytes()
}

func (r Row) String() string {
	email := bytes.Trim(r.Email[:], "\x00")
	name := bytes.Trim(r.UserName[:], "\x00")
	return fmt.Sprintf(`{"id": %d, "user_name": "%s", "email": "%s"}`, r.ID, name, email)
}

func NewTable() *Table {
	t := &Table{}
	return t
}

var row Row

// column size
const (
	ID_OFFSET       uint32 = 0
	ID_SIZE                = uint32(unsafe.Sizeof(row.ID))
	USERNAME_SIZE          = uint32(unsafe.Sizeof(row.UserName))
	EMAIL_SIZE             = uint32(unsafe.Sizeof(row.Email))
	USERNAME_OFFSET        = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET           = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE               = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

const (
	PAGE_SIZE       uint32 = 4096                            // ページサイズ
	TABLE_MAX_PAGES uint32 = 100                             // 最大ページ数
	ROWS_PER_PAGE          = PAGE_SIZE / ROW_SIZE            // 1ページあたりのレコード数、14レコード(4096/291)
	TABLE_MAX_ROWS         = ROWS_PER_PAGE * TABLE_MAX_PAGES // 全ページで保存できるレコード数
)

type Page [PAGE_SIZE]byte

func (p Page) String() string {
	builder := strings.Builder{}
	builder.WriteString("[")
	for i := 0; i < int(ROWS_PER_PAGE); i++ {
		offset := uint32(i) * ROW_SIZE
		var row Row
		if err := DeserializeRow(&p, offset, &row); err != nil {
			return err.Error()
		}
		builder.WriteString(row.String())

		if i+1 < int(ROWS_PER_PAGE) {
			builder.WriteString(",")
		}
	}
	builder.WriteString("]")
	return builder.String()
}

type Table struct {
	NumRow uint32 // 合計レコード数
	Pages  [TABLE_MAX_PAGES]*Page
}

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota + 1
	STATEMENT_SELECT
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota + 1
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)

func RowSlot(table *Table, rowNum uint32) (*Page, uint32) {
	// 合計レコード数 / 1ページあたりのレコード数 = ページのindex
	// 例）1ページで保存できるレコード数は3件だとすると
	// 1/3 = 0
	// 3/3 = 1
	// 6/3 = 2
	// ...
	pageNum := rowNum / ROWS_PER_PAGE
	page := table.Pages[pageNum]
	if page == nil {
		table.Pages[pageNum] = &Page{}
		page = table.Pages[pageNum]
	}

	// 合計レコード数 % 1ページあたりのレコード数 = ページ内のレコードのindex
	// 例） 1%3 = 1
	rowOffset := rowNum % ROWS_PER_PAGE
	return page, rowOffset
}

func SerializeRow(row Row, page *Page, rowOffset uint32) {
	start := rowOffset * ROW_SIZE
	end := start + ROW_SIZE
	copy(page[start:end], row.Bytes())
}

func DeserializeRow(page *Page, rowOffset uint32, row *Row) error {
	start := rowOffset * ROW_SIZE
	end := start + ROW_SIZE
	buf := bytes.NewReader(page[start:end])
	if err := binary.Read(buf, binary.BigEndian, row); err != nil {
		return err
	}
	return nil
}

func ExecuteInsert(stmt *Statement, table *Table) ExecuteResult {
	if table.NumRow > TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	row := stmt.RowToInsert
	page, rowOffset := RowSlot(table, table.NumRow)
	SerializeRow(row, page, rowOffset)
	table.NumRow++
	return EXECUTE_SUCCESS
}

func ExecuteSelect(stmt *Statement, table *Table) ExecuteResult {
	for i := 0; i < int(table.NumRow); i++ {
		page, rowOffset := RowSlot(table, uint32(i))
		var row Row
		if err := DeserializeRow(page, rowOffset, &row); err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(row)
	}
	return EXECUTE_SUCCESS
}

func DoMetaCommand(cmd string) MetaCommandResult {
	if strings.HasSuffix(cmd, ".exit") {
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func scanInput(line string, row *Row) int {
	col := strings.Split(line, " ")
	if len(col) != 4 {
		return len(col)
	}

	id, err := strconv.ParseUint(col[1], 10, 32)
	if err != nil {
		log.Println(err)
		return 0
	}
	row.ID = uint32(id)

	if copy(row.UserName[:], []byte(col[2])) == 0 {
		return 1
	}

	if copy(row.Email[:], []byte(col[3])) == 0 {
		return 2
	}

	return len(col)
}

func PrepareStatement(line string, stmt *Statement) PrepareResult {
	if len(line) >= 6 && line[:6] == "insert" {
		stmt.Type = STATEMENT_INSERT
		argsAssigned := scanInput(line, &stmt.RowToInsert)
		if argsAssigned < 3 {
			return PREPARE_SYNTAX_ERROR
		}
		return PREPARE_SUCCESS
	}
	if len(line) >= 6 && line[:6] == "select" {
		stmt.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func ExecuteStatement(stmt *Statement, table *Table) ExecuteResult {
	switch stmt.Type {
	case STATEMENT_INSERT:
		return ExecuteInsert(stmt, table)
	case STATEMENT_SELECT:
		return ExecuteSelect(stmt, table)
	}
	return EXECUTE_SUCCESS
}

func main() {
	table := NewTable()
	current := console.Current()
	if err := current.DisableEcho(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() {
		_ = current.Reset()
	}()

	if err := current.SetRaw(); err != nil {
		log.Println(err)
		return
	}

	term := term.NewTerminal(current, "db > ")

	for {
		line, err := term.ReadLine()
		if err != nil {
			break
		}

		if line[0] == '.' {
			switch DoMetaCommand(line) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", line)
				continue
			}
		}

		var stmt Statement
		switch PrepareStatement(line, &stmt) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", line)
			continue
		}

		switch ExecuteStatement(&stmt, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
		}
	}
}
