### Author: Marwan Al-Soltany <MarwanAlsoltany@gmail.com>
### Usage: make <command> (run [make help] for more information)

.EXPORT_ALL_VARIABLES:
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Default values for environment variables (can be overridden by .env or shell)
VENDOR ?= vendor
PROJECT ?= project
VERSION ?= latest

.DEFAULT_GOAL := help

.PHONY: help \
	build test test-coverage test-integration lint format tidy check clean

# ---------------------

help: ## Show this help message
	@echo "\n\033[31m${VENDOR}/${PROJECT}\033[0m Task Runner"
	@echo "\nUsage:\n\t\033[33mmake\033[0m [command]\n"
	@echo "\nCommands:"
	@grep -E '^[a-zA-Z%_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\t\033[36m%-18s\033[0m -> %s\n", $$1, $$2}'; \
		echo '';

confirm:
	@if test -z CONFIRM; then echo -n ''; else echo "${CONFIRM}"; fi
	@read -p "Are you sure? [y/N]: " answer && answer=$${answer:-N}; \
	if [ $${answer} = y ] || [ $${answer} = Y ]; \
	then \
		printf "\033[32m%s\033[0m\n" "OK"; \
	else \
		printf "\033[31m%s\033[0m\n" "Aborting ..."; \
		exit 1; \
	fi

check-env-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable '$*' is not defined!"; \
		exit 1; \
	fi

# ---------------------

build: ## Build the project
	@echo "Building ..."
	go build -v ./...

test: ## Test the project
	@echo "Testing ..."
	go test -v -race -count=1 ./...

test-coverage: ## Test the project with coverage report
	@echo "Testing with coverage ..."
	go test -v -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

test-integration: ## Test the project with integration tag and coverage report
	@echo "Running integration tests ..."
	go test -v -race -count=1 -tags=integration -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint: ## Lint the project
	@echo "Linting ..."
	go vet ./...

format: ## Format the project
	@echo "Formatting ..."
	go fmt ./...

tidy: ## Tidy and verify project dependencies
	@echo "Tidying ..."
	go mod tidy
	go mod verify

check: ## Check project dependencies for updates (direct and outdated)
	@echo "Checking for outdated dependencies ..."
	@out=$$(go list -u -m -json all | jq -r 'select(.Main != true and .Indirect != true and .Update != null) | "\(.Path) \((if .Version == null then "(devel)" else .Version end)) -> \(.Update.Version)"'); \
	if [ -z "$$out" ]; then \
		echo "All modules up to date"; \
	else \
		echo "$$out"; \
	fi

clean: ## Clean build artifacts
	@echo "Cleaning ..."
	go clean
	rm -f coverage.*
