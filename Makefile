.PHONY: all up down test load chaos status logs clean help

PYTHON := .venv/bin/python
PYTEST := .venv/bin/pytest

all: up

up:
	@if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then \
		echo "Starting services via Docker Compose..."; \
		docker compose up --build -d; \
	else \
		echo "Starting services via native dev stack..."; \
		./scripts/dev_stack.sh start; \
	fi

down:
	@if command -v docker >/dev/null 2>&1 && docker compose ps -q 2>/dev/null | grep -q .; then \
		echo "Stopping Docker Compose services..."; \
		docker compose down; \
	fi
	@./scripts/dev_stack.sh stop

status:
	@if command -v docker >/dev/null 2>&1 && docker compose ps -q 2>/dev/null | grep -q .; then \
		docker compose ps; \
	else \
		./scripts/dev_stack.sh status; \
	fi

test:
	@echo "Running test suite (unit + integration)..."
	$(PYTEST) tests/ -v

load:
	@echo "Running load test suite..."
	$(PYTHON) scripts/load_test.py --jobs 1000 --concurrency 16

chaos:
	@echo "Running chaos engineering test suite..."
	$(PYTHON) scripts/chaos_test.py --jobs 500

clean:
	@./scripts/dev_stack.sh clean
	@rm -rf .pytest_cache/ htmlcov/ .coverage
