# =============================================================================
# Greengraph — Context Graph for RAG Retrieval
# =============================================================================
# Usage:
#   make install    — set up the Python environment
#   make run        — start the database (Docker) and open a CLI demo
#   make deploy     — build and start all services in the background
#   make test       — run unit tests
#   make test-all   — run unit + integration tests (requires running DB)
#   make lint       — ruff lint check
#   make format     — ruff format (in-place)
#   make typecheck  — ty type check
#   make check      — lint + typecheck (CI gate)
#   make db-shell   — open a psql shell
#   make db-status  — show DB/extension status
#   make backup     — dump the database to backups/
#   make down       — stop Docker services
#   make clean      — remove venv and caches
# =============================================================================

.DEFAULT_GOAL := help
SHELL         := /bin/bash

# Paths
VENV          := .venv
UV            := uv
PYTHON        := $(VENV)/bin/python
PYTEST        := $(VENV)/bin/pytest
RUFF          := $(VENV)/bin/ruff
TY            := $(VENV)/bin/ty

# Docker
COMPOSE       := docker compose
DB_CONTAINER  := context-graph-db
DB_USER       := context_graph
DB_NAME       := context_graph_db

# Colours
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RESET  := \033[0m

.PHONY: help
help:  ## Show this help message
	@echo ""
	@echo "  $(GREEN)Greengraph — Context Graph for RAG Retrieval$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# =============================================================================
# Setup
# =============================================================================

.PHONY: install
install: ## Install Python dependencies into .venv using uv
	$(UV) sync --all-groups
	@echo "$(GREEN)Installation complete. Activate with: source $(VENV)/bin/activate$(RESET)"

.PHONY: env
env: ## Copy .env.example to .env if .env does not exist
	@if [ ! -f .env ]; then \
	  cp .env.example .env; \
	  echo "$(YELLOW)Created .env — edit it to set your credentials.$(RESET)"; \
	else \
	  echo ".env already exists"; \
	fi

# =============================================================================
# Docker / Deployment
# =============================================================================

.PHONY: deploy
deploy: env ## Build image and start all services in the background (production-like)
	$(COMPOSE) up -d --build
	@echo "$(GREEN)Services started. Run 'make db-status' to verify.$(RESET)"

.PHONY: run
run: env ## Start the database (if not running) and show a live status
	@echo "$(GREEN)Starting context-graph-db...$(RESET)"
	$(COMPOSE) up -d
	@echo "Waiting for PostgreSQL to be healthy..."
	@for i in $$(seq 1 30); do \
	  $(COMPOSE) exec $(DB_CONTAINER) pg_isready -U $(DB_USER) -d $(DB_NAME) 2>/dev/null && break; \
	  sleep 2; \
	done
	@$(MAKE) db-status
	@echo ""
	@echo "$(GREEN)Database is ready. Run commands via:$(RESET)"
	@echo "  $(VENV)/bin/greengraph --help"
	@echo "  $(VENV)/bin/greengraph db status"
	@echo "  $(VENV)/bin/greengraph ingest <file>"
	@echo "  $(VENV)/bin/greengraph retrieve '<query>'"

.PHONY: down
down: ## Stop Docker services (data is preserved)
	$(COMPOSE) down
	@echo "$(GREEN)Services stopped.$(RESET)"

.PHONY: restart
restart: ## Restart Docker services
	$(COMPOSE) restart

.PHONY: destroy
destroy: ## DANGER: stop services and delete all data volumes
	@echo "$(YELLOW)WARNING: This will destroy all database data!$(RESET)"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	$(COMPOSE) down -v
	@echo "$(GREEN)All services and volumes removed.$(RESET)"

.PHONY: logs
logs: ## Tail Docker logs
	$(COMPOSE) logs -f

# =============================================================================
# Database helpers
# =============================================================================

.PHONY: db-status
db-status: ## Show PostgreSQL connection, extension, and graph status
	$(VENV)/bin/greengraph db status

.PHONY: db-shell
db-shell: ## Open an interactive psql shell
	$(COMPOSE) exec $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME)

.PHONY: backup
backup: ## Dump the full database to backups/
	bash backup.sh

.PHONY: ingest-zip
ingest-zip: ## Ingest a Webhose news zip: make ingest-zip ZIP=path/to/file.zip
	@test -n "$(ZIP)" || (echo "Usage: make ingest-zip ZIP=path/to/file.zip" && exit 1)
	$(PYTHON) scripts/ingest_webhose_zip.py $(ZIP) $(ARGS)

# =============================================================================
# Testing
# =============================================================================

.PHONY: test
test: ## Run unit tests only (no DB required)
	$(PYTEST) -m "not integration" --cov=greengraph --cov-report=term-missing

.PHONY: test-all
test-all: ## Run all tests including integration tests (requires running DB)
	$(PYTEST) --cov=greengraph --cov-report=term-missing

.PHONY: test-integration
test-integration: ## Run integration tests only (requires running DB)
	$(PYTEST) -m integration -v

.PHONY: test-cov
test-cov: ## Run unit tests with HTML coverage report
	$(PYTEST) -m "not integration" --cov=greengraph --cov-report=html
	@echo "$(GREEN)Coverage report: htmlcov/index.html$(RESET)"

# =============================================================================
# Code quality
# =============================================================================

.PHONY: lint
lint: ## Run ruff linter (check only)
	$(RUFF) check src/ tests/

.PHONY: lint-fix
lint-fix: ## Run ruff linter and auto-fix safe issues
	$(RUFF) check --fix src/ tests/

.PHONY: format
format: ## Run ruff formatter (in-place)
	$(RUFF) format src/ tests/

.PHONY: format-check
format-check: ## Check formatting without making changes
	$(RUFF) format --check src/ tests/

.PHONY: typecheck
typecheck: ## Run ty type checker
	$(TY) check src/

.PHONY: check
check: lint format-check typecheck ## Run all quality checks (CI gate)
	@echo "$(GREEN)All checks passed.$(RESET)"

# =============================================================================
# Utility
# =============================================================================

.PHONY: clean
clean: ## Remove venv, caches, and build artifacts
	rm -rf $(VENV) .ruff_cache .pytest_cache htmlcov __pycache__ dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Clean complete.$(RESET)"

.PHONY: ps
ps: ## Show running Docker containers
	$(COMPOSE) ps
