#!/bin/bash

# Airflow Stop Script for Weather Observations and Forecasting System

echo "🛑 Stopping Airflow Orchestration"
echo "================================="

# Check if we're in the right directory
if [ ! -f "docker-compose.airflow.yml" ]; then
    echo "❌ Error: docker-compose.airflow.yml not found. Please run this script from the airflow/ directory."
    exit 1
fi

# Stop services
echo "🛑 Stopping Airflow services..."
docker-compose -f docker-compose.airflow.yml down

echo "✅ Airflow services stopped successfully!"
echo ""
echo "💡 To start again: ./start_airflow.sh"
echo "🗑️  To remove volumes: docker-compose -f docker-compose.airflow.yml down -v"
