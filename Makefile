# Weather Observations and Forecasting System - Makefile
# Simple commands for Airflow orchestration

.PHONY: help start stop status logs clean

# Default target
help: ## Show this help message
	@echo "Weather Observations and Forecasting System"
	@echo "=========================================="
	@echo ""
	@echo "Available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick Start:"
	@echo "  make start    # Start Airflow orchestration"
	@echo "  make stop     # Stop Airflow orchestration"
	@echo ""

# Airflow Orchestration
start: ## Start Airflow orchestration (runs twice for proper initialization)
	@echo "🚀 Starting Airflow orchestration..."
	@echo "📋 First startup run (initialization)..."
	@cd airflow && ./start_airflow.sh
	@echo "⏳ Waiting 10 seconds for services to stabilize..."
	@sleep 10
	@echo "🔄 Second startup run (ensuring all services are ready)..."
	@cd airflow && ./start_airflow.sh
	@echo "✅ Airflow orchestration startup complete!"

stop: ## Stop Airflow orchestration
	@echo "🛑 Stopping Airflow orchestration..."
	@cd airflow && ./stop_airflow.sh

status: ## Show Airflow service status
	@echo "📊 Airflow Services Status:"
	@cd airflow && ./status_airflow.sh

logs: ## Show Airflow logs
	@echo "📋 Airflow Logs:"
	@cd airflow && docker-compose -f docker-compose.airflow.yml logs -f

clean: ## Clean up containers and volumes
	@echo "🧹 Cleaning up..."
	@cd airflow && docker-compose -f docker-compose.airflow.yml down -v
	@echo "✅ Cleanup completed"