# Contributing

Bug reports and pull requests are welcome at [github.com/MarwanAlsoltany/amqp-broker](https://github.com/MarwanAlsoltany/amqp-broker).

## Requirements

- Go >= 1.25

## Getting started

```sh
git clone https://github.com/MarwanAlsoltany/amqp-broker
cd amqp-broker
go test -race ./...
```

Or use the `Makefile` targets:

```sh
make test            # run tests with -race
make test-coverage   # run tests and open coverage report
make lint            # run go vet
make format          # run go fmt
make tidy            # run go mod tidy && go mod verify
```

## Guidelines

**Scope:** Keep changes focused. A bug fix should not reach beyond the minimal code path needed to fix the issue. New features should be discussed in an issue before a pull request is opened.

**API surface:** The package API is intentionally small. New public symbols require a clear use case that cannot be satisfied by composing existing primitives.

**Coverage:** All new code must be covered by tests. `go test -cover ./...` must remain at 100% statement coverage.

**Documentation:** Public symbols must have a doc comment. Follow the existing style: full sentences, no em dashes, `->` instead of arrow symbols.

**Formatting:** Run `go fmt ./...` before committing. The CI workflow will reject unformatted code.

**Changelog:** Update `CHANGELOG.md` under `## [Unreleased]` for any user-visible change (new feature, bug fix, breaking change, deprecation).

## Commit messages

Use the conventional commits format:

```txt
feat(domain): add Foo method
fix(format): handle nil leaf error
docs: clarify Derive vs Detail distinction
test: add example for AllDataAs
```

## Reporting security vulnerabilities

Do not open a public GitHub issue for security vulnerabilities. See [SECURITY.md](./SECURITY.md) instead.
