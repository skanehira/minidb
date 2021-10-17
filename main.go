package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	EXECUTE_DESERIALIZE_FAIL
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

type (
	UserName [COLUMN_USERNAME_SIZE]byte
	Email    [COLUMN_EMAIL_SIZE]byte
)

type Row struct {
	ID       uint32
	UserName UserName
	Email    Email
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

func DBOpen(fileName string) *Table {
	pager := PagerOpen(fileName)
	numRow := pager.FileLength / ROW_SIZE
	t := &Table{
		NumRow: numRow,
		Pager:  pager,
	}
	return t
}

func DBClose(table *Table) {
	pager := table.Pager
	// 現時点のMaxページ数
	numFullPages := table.NumRow / ROWS_PER_PAGE

	for i := 0; i < int(numFullPages); i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(pager, int64(i), int64(PAGE_SIZE))
	}

	numAdditionalRows := table.NumRow % ROWS_PER_PAGE
	if numAdditionalRows > 0 {
		pageNum := numFullPages

		if pager.Pages[pageNum] != nil {
			pagerFlush(pager, int64(pageNum), int64(numAdditionalRows*ROW_SIZE))
		}
	}

	if err := pager.File.Close(); err != nil {
		exitError(fmt.Errorf("error closing db file: %w\n", err))
	}
}

func pagerFlush(pager *Pager, pageNum, size int64) {
	if pager.Pages[pageNum] == nil {
		exitError("tried to flush null page")
	}
	if _, err := pager.File.Seek(pageNum*int64(PAGE_SIZE), 0); err != nil {
		exitError(fmt.Sprintf("error seeking: %s\n", err))
	}
	if _, err := pager.File.Write(pager.Pages[pageNum][:size]); err != nil {
		exitError(fmt.Sprintf("error writing: %s\n", err))
	}
}

func PagerOpen(fileName string) *Pager {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		exitError(fmt.Sprintf("unable to open file: %s\n", fileName))
	}

	fi, err := os.Stat(fileName)
	if err != nil {
		exitError(fmt.Sprintf("unable to get file info: %s\n", fileName))
	}

	pager := &Pager{
		FileLength: uint32(fi.Size()),
	}

	pager.File = f

	return pager
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

type DBFile interface {
	io.ReadWriteSeeker
	io.Closer
}

type Pager struct {
	File       DBFile
	FileLength uint32
	Pages      [TABLE_MAX_PAGES]*Page
}

type Table struct {
	NumRow uint32 // 合計レコード数
	Pager  *Pager
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

func GetPage(pager *Pager, pageNum uint32) *Page {
	if pageNum > TABLE_MAX_PAGES {
		exitError(fmt.Sprintf("tried to fetch page number out of bounds: %d\n", TABLE_MAX_PAGES))
	}

	page := pager.Pages[pageNum]
	if page == nil {
		// メモリにページが存在しない場合は初期化
		pager.Pages[pageNum] = &Page{}
		page = pager.Pages[pageNum]
	}

	// 現在のページ数を取得
	numPages := pager.FileLength / PAGE_SIZE
	// あまりがある場合は、+1ページ
	if pager.FileLength%PAGE_SIZE != 0 {
		numPages++
	}

	// 今のページが現在のページ数内に収まる場合はファイルからデータを取得
	if pageNum < numPages {
		// ページのoffsetにシーク
		_, err := pager.File.Seek(int64(pageNum*PAGE_SIZE), 0)
		if err != nil {
			exitError(fmt.Sprintf("error seeking: %s\n", err))
		}

		_, err = pager.File.Read(page[:])
		if err != nil {
			exitError(fmt.Sprintf("error reading: %s\n", err))
		}

		// ページデータを読む取る
	}

	return page
}

func RowSlot(table *Table, rowNum uint32) (*Page, uint32) {
	// 合計レコード数 / 1ページあたりのレコード数 = ページのindex
	// 例）1ページで保存できるレコード数は3件だとすると
	// 1/3 = 0
	// 3/3 = 1
	// 6/3 = 2
	// ...
	pageNum := rowNum / ROWS_PER_PAGE

	page := GetPage(table.Pager, pageNum)

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
	data := page[start:end]
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.BigEndian, row); err != nil {
		return err
	}
	return nil
}

func ExecuteInsert(stmt *Statement, table *Table) ExecuteResult {
	if table.NumRow >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	row := stmt.RowToInsert
	page, rowOffset := RowSlot(table, table.NumRow)
	SerializeRow(row, page, rowOffset)
	table.NumRow++
	return EXECUTE_SUCCESS
}

func ExecuteSelect(stmt *Statement, table *Table) ([]Row, ExecuteResult) {
	var rows []Row
	for i := 0; i < int(table.NumRow); i++ {
		page, rowOffset := RowSlot(table, uint32(i))
		if err := DeserializeRow(page, rowOffset, &row); err != nil {
			return nil, EXECUTE_DESERIALIZE_FAIL
		}
		rows = append(rows, row)
	}
	return rows, EXECUTE_SUCCESS
}

func DoMetaCommand(cmd string, table *Table) MetaCommandResult {
	if strings.HasSuffix(cmd, ".exit") {
		DBClose(table)
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

var (
	ErrTooManyRows     = errors.New("too many rows")
	ErrStringIsTooLong = errors.New("string is too long")
	ErrDataIsEmpty     = errors.New("data is empty")
)

func ScanInput(line string, row *Row) error {
	col := strings.Split(line, " ")
	if len(col) != 4 {
		return ErrTooManyRows
	}

	id, err := strconv.ParseUint(col[1], 10, 32)
	if err != nil {
		return fmt.Errorf("id is is not a number: %w", err)
	}
	row.ID = uint32(id)

	if len(col[2]) > COLUMN_USERNAME_SIZE {
		return fmt.Errorf("invalid user name: %w", ErrStringIsTooLong)
	}
	copy(row.UserName[:], []byte(col[2]))

	if len(col[3]) > COLUMN_EMAIL_SIZE {
		return fmt.Errorf("invalid email: %w", ErrStringIsTooLong)
	}
	copy(row.Email[:], []byte(col[3]))

	return nil
}

func PrepareStatement(line string, stmt *Statement) PrepareResult {
	if len(line) >= 6 && line[:6] == "insert" {
		stmt.Type = STATEMENT_INSERT
		err := ScanInput(line, &stmt.RowToInsert)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
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
		rows, result := ExecuteSelect(stmt, table)
		if result != EXECUTE_SUCCESS {
			return result
		}
		for _, row := range rows {
			fmt.Println(row)
		}
	}
	return EXECUTE_SUCCESS
}

func exitError(err interface{}) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 2 {
		exitError("must supply a database filename.\n")
	}
	table := DBOpen(os.Args[1])
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
			switch DoMetaCommand(line, table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("unrecognized command '%s'.\n", line)
				continue
			}
		}

		var stmt Statement
		switch PrepareStatement(line, &stmt) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Fprintln(os.Stderr, "syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Fprintf(os.Stderr, "unrecognized keyword at start of '%s'.\n", line)
			continue
		}

		switch ExecuteStatement(&stmt, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
		case EXECUTE_TABLE_FULL:
			fmt.Println("error: table full.")
		}
	}
}
