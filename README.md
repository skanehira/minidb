![GitHub Repo stars](https://img.shields.io/github/stars/skanehira/minidb?style=social)
![GitHub](https://img.shields.io/github/license/skanehira/minidb)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/skanehira/minidb)
![GitHub all releases](https://img.shields.io/github/downloads/skanehira/minidb/total)
![GitHub CI Status](https://img.shields.io/github/workflow/status/skanehira/minidb/ci?label=CI)
![GitHub Release Status](https://img.shields.io/github/workflow/status/skanehira/minidb/Release?label=release)

# minidb
This is template that help you to quick implement some CLI using Go.

This repository is contains following.

- minimal CLI implementation using [spf13/cobra](https://github.com/spf13/cobra)
- CI/CD
  - golangci-lint
  - go test
  - goreleaser
  - dependabot for github-actions and Go
  - CodeQL Analysis (Go)

## How to use
1. fork this repository
2. replace `skanehira` to your user name using `sed`(or others)
3. run `make init`

## Author
skanehira
