#!/bin/bash

# Airflow Startup Script for Weather Observations and Forecasting System

set -e

echo "🚀 Starting Airflow Orchestration for Weather Data Pipeline"
echo "=========================================================="

# Check if we're in the right directory
if [ ! -f "docker-compose.airflow.yml" ]; then
    echo "❌ Error: docker-compose.airflow.yml not found. Please run this script from the airflow/ directory."
    exit 1
fi

# Check if .env file exists in parent directory
if [ ! -f "../.env" ]; then
    echo "❌ Error: .env file not found in parent directory. Please create it first."
    exit 1
fi

echo "📋 Checking prerequisites..."
echo "✅ Docker Compose file found"
echo "✅ Environment file found"

# Stop any existing services
echo "🛑 Stopping any existing Airflow services..."
docker-compose -f docker-compose.airflow.yml down

# Start services
echo "🚀 Starting Airflow services..."
docker-compose -f docker-compose.airflow.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to initialize..."
sleep 10

# Check service status
echo "📊 Service Status:"
docker-compose -f docker-compose.airflow.yml ps

echo ""
echo "🎉 Airflow is starting up!"
echo ""
echo "📱 Access the Airflow Web UI at: http://localhost:8080"
echo "👤 Username: admin"
echo "🔑 Password: admin"
echo ""
echo "📋 Available DAGs:"
echo "   1. weather_onetime_setup - Run this first (manual trigger)"
echo "   2. weather_hourly_ingestion - Runs automatically every hour"
echo ""
echo "📖 For detailed instructions, see: airflow/README.md"
echo ""
echo "🔍 To view logs: docker-compose -f docker-compose.airflow.yml logs"
echo "🛑 To stop: docker-compose -f docker-compose.airflow.yml down"
echo ""
echo "✨ Happy orchestrating!"
