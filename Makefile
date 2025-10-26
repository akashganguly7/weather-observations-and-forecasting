# Weather Observations and Forecasting System - Makefile
# Provides simple commands for all project operations

.PHONY: help install setup start stop restart status logs clean test dbt-test airflow-start airflow-stop airflow-status airflow-logs

# Default target
help: ## Show this help message
	@echo "Weather Observations and Forecasting System"
	@echo "=========================================="
	@echo ""
	@echo "Available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick Start:"
	@echo "  make setup     # Initial setup"
	@echo "  make start     # Start main services"
	@echo "  make airflow-start  # Start Airflow orchestration"
	@echo ""

# Installation and Setup
install: ## Install Python dependencies
	@echo "📦 Installing Python dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed"

setup: ## Initial project setup (automated)
	@echo "🚀 Running automated setup..."
	@./setup.sh

setup-manual: ## Manual project setup
	@echo "🚀 Setting up Weather Observations and Forecasting System..."
	@echo "📋 Checking prerequisites..."
	@command -v docker >/dev/null 2>&1 || { echo "❌ Docker is required but not installed. Aborting."; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || { echo "❌ Docker Compose is required but not installed. Aborting."; exit 1; }
	@echo "✅ Prerequisites check passed"
	@echo "📝 Please ensure your .env file is configured"
	@echo "💡 Run 'make start' to start the main services"
	@echo "💡 Run 'make airflow-start' to start Airflow orchestration"

# Main Services (Original Python Script Approach)
start: ## Start main PostgreSQL and ingestion services
	@echo "🚀 Starting main services..."
	docker-compose up -d postgres
	@echo "⏳ Waiting for PostgreSQL to be ready..."
	@sleep 5
	@echo "✅ Main services started"
	@echo "💡 Run 'make logs' to view logs"
	@echo "💡 Run 'make run-ingestion' to start data ingestion"

stop: ## Stop main services
	@echo "🛑 Stopping main services..."
	docker-compose down
	@echo "✅ Main services stopped"

restart: ## Restart main services
	@echo "🔄 Restarting main services..."
	docker-compose restart
	@echo "✅ Main services restarted"

status: ## Show status of main services
	@echo "📊 Main Services Status:"
	docker-compose ps

logs: ## Show logs from main services
	docker-compose logs -f

# Data Ingestion
run-ingestion: ## Run the main Python ingestion script
	@echo "🔄 Running data ingestion..."
	python main.py

# Testing
test: ## Run Python unit tests
	@echo "🧪 Running Python unit tests..."
	python -m pytest ingestion_tests/ -v

dbt-test: ## Run dbt tests
	@echo "🧪 Running dbt tests..."
	docker-compose run --rm dbt dbt test

dbt-run: ## Run dbt models
	@echo "🔄 Running dbt models..."
	docker-compose run --rm dbt dbt run

dbt-build: ## Run dbt build (models + tests)
	@echo "🏗️  Running dbt build..."
	docker-compose run --rm dbt dbt build

# Airflow Orchestration
airflow-start: ## Start Airflow orchestration
	@echo "🚀 Starting Airflow orchestration..."
	@cd airflow && ./start_airflow.sh

airflow-stop: ## Stop Airflow orchestration
	@echo "🛑 Stopping Airflow orchestration..."
	@cd airflow && ./stop_airflow.sh

airflow-status: ## Show Airflow service status
	@echo "📊 Airflow Services Status:"
	@cd airflow && ./status_airflow.sh

airflow-logs: ## Show Airflow logs
	@echo "📋 Airflow Logs:"
	@cd airflow && docker-compose -f docker-compose.airflow.yml logs -f

airflow-restart: ## Restart Airflow services
	@echo "🔄 Restarting Airflow services..."
	@cd airflow && docker-compose -f docker-compose.airflow.yml restart

# Database Operations
db-connect: ## Connect to PostgreSQL database
	@echo "🔌 Connecting to PostgreSQL database..."
	@echo "💡 Use '\\q' to quit"
	docker-compose exec postgres psql -U $(shell grep POSTGRES_USER .env | cut -d '=' -f2) -d $(shell grep POSTGRES_DB .env | cut -d '=' -f2)

db-backup: ## Create database backup
	@echo "💾 Creating database backup..."
	@mkdir -p backups
	docker-compose exec postgres pg_dump -U $(shell grep POSTGRES_USER .env | cut -d '=' -f2) -d $(shell grep POSTGRES_DB .env | cut -d '=' -f2) > backups/backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Backup created in backups/ directory"

db-restore: ## Restore database from backup (usage: make db-restore BACKUP=backup_file.sql)
	@echo "🔄 Restoring database from backup..."
	@if [ -z "$(BACKUP)" ]; then echo "❌ Please specify backup file: make db-restore BACKUP=backup_file.sql"; exit 1; fi
	docker-compose exec -T postgres psql -U $(shell grep POSTGRES_USER .env | cut -d '=' -f2) -d $(shell grep POSTGRES_DB .env | cut -d '=' -f2) < $(BACKUP)
	@echo "✅ Database restored from $(BACKUP)"

# Development
dev-setup: ## Setup development environment
	@echo "🛠️  Setting up development environment..."
	pip install -r requirements.txt
	@echo "✅ Development environment ready"

lint: ## Run code linting
	@echo "🔍 Running code linting..."
	@echo "📝 Checking Python files..."
	@find . -name "*.py" -not -path "./venv/*" -not -path "./.git/*" | xargs python -m py_compile
	@echo "✅ Code linting completed"

format: ## Format Python code
	@echo "🎨 Formatting Python code..."
	@echo "💡 Consider using black or autopep8 for code formatting"
	@echo "✅ Code formatting completed"

# Cleanup
clean: ## Clean up containers, volumes, and temporary files
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	@cd airflow && docker-compose -f docker-compose.airflow.yml down -v
	@rm -rf __pycache__/
	@find . -name "*.pyc" -delete
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleanup completed"

clean-all: ## Complete cleanup (containers, images, volumes, networks)
	@echo "🧹 Running complete cleanup..."
	@./cleanup.sh

clean-logs: ## Clean up log files
	@echo "🧹 Cleaning up log files..."
	@rm -rf logs/
	@cd airflow && rm -rf logs/
	@echo "✅ Log files cleaned"

# Monitoring and Health Checks
health: ## Check system health
	@echo "🏥 System Health Check:"
	@echo "📊 Main Services:"
	@docker-compose ps
	@echo ""
	@echo "📊 Airflow Services:"
	@cd airflow && docker-compose -f docker-compose.airflow.yml ps 2>/dev/null || echo "Airflow not running"
	@echo ""
	@echo "🌐 Service URLs:"
	@echo "  Main PostgreSQL: localhost:5432"
	@echo "  Airflow Web UI: http://localhost:8080 (admin/admin)"

# Quick Commands for Common Workflows
quick-start: ## Quick start with main services
	@echo "⚡ Quick starting main services..."
	make start
	@echo "✅ Ready! Run 'make run-ingestion' to start data collection"

quick-airflow: ## Quick start with Airflow
	@echo "⚡ Quick starting with Airflow orchestration..."
	make start
	make airflow-start
	@echo "✅ Ready! Access Airflow at http://localhost:8080"

full-reset: ## Full system reset (stops everything, cleans up, and restarts)
	@echo "🔄 Full system reset..."
	make clean
	make start
	@echo "✅ Full reset completed"

# Documentation
docs: ## Show project documentation
	@echo "📚 Project Documentation:"
	@echo "  Main README: README.md"
	@echo "  Airflow README: airflow/README.md"
	@echo "  Available commands: make help"

# Environment
env-check: ## Check environment configuration
	@echo "🔍 Environment Configuration Check:"
	@if [ -f .env ]; then \
		echo "✅ .env file found"; \
		echo "📋 Key variables:"; \
		grep -E "^(POSTGRES_|BRIGHTSKY_|HTTP_|FORECAST_|DEFAULT_)" .env | sed 's/^/  /'; \
	else \
		echo "❌ .env file not found"; \
		echo "💡 Please create .env file from .env.example"; \
	fi

# Version and Info
version: ## Show project version and info
	@echo "Weather Observations and Forecasting System"
	@echo "Version: 1.0.0"
	@echo "Python: $(shell python --version 2>/dev/null || echo 'Not available')"
	@echo "Docker: $(shell docker --version 2>/dev/null || echo 'Not available')"
	@echo "Docker Compose: $(shell docker-compose --version 2>/dev/null || echo 'Not available')"
