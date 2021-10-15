package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/containerd/console"
	"golang.org/x/crypto/ssh/terminal"
)

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

		if strings.HasSuffix(line, ".exit") {
			break
		}
		fmt.Printf("Unrecognized command '%s'.\n", line)
	}
}
