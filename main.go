package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/containerd/console"
	"golang.org/x/crypto/ssh/terminal"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota + 1
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type Statement struct {
	Type StatementType
}

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota + 1
	STATEMENT_SELECT
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota + 1
	PREPARE_UNRECOGNIZED_STATEMENT
)

func doMetaCommand(cmd string) MetaCommandResult {
	if strings.HasSuffix(cmd, ".exit") {
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(line string, stmt *Statement) PrepareResult {
	if len(line) >= 6 && line[:6] == "insert" {
		stmt.Type = STATEMENT_INSERT
		return PREPARE_SUCCESS
	}
	if len(line) >= 6 && line[:6] == "select" {
		stmt.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(stmt *Statement) error {
	switch stmt.Type {
	case STATEMENT_INSERT:
		println("This is where we would do an insert.")
	case STATEMENT_SELECT:
		println("This is where we would do a select.")
	}
	return nil
}

func main() {
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

	term := terminal.NewTerminal(current, "db > ")

	for {
		line, err := term.ReadLine()
		if err != nil {
			break
		}

		if line[0] == '.' {
			switch doMetaCommand(line) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", line)
				continue
			}
		}

		var stmt Statement
		switch prepareStatement(line, &stmt) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", line)
			continue
		}

		if err := executeStatement(&stmt); err != nil {
			fmt.Println(err)
			continue
		}

		println("Executed.")
	}
}
