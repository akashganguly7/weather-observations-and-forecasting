#!/bin/bash

# Airflow Status Check Script for Weather Observations and Forecasting System

echo "📊 Airflow Service Status"
echo "========================"

# Check if we're in the right directory
if [ ! -f "docker-compose.airflow.yml" ]; then
    echo "❌ Error: docker-compose.airflow.yml not found. Please run this script from the airflow/ directory."
    exit 1
fi

# Check service status
echo "🔍 Checking service status..."
docker-compose -f docker-compose.airflow.yml ps

echo ""
echo "🌐 Airflow Web UI: http://localhost:8080"
echo "👤 Username: admin | Password: admin"
echo ""

# Check if services are healthy
echo "🏥 Health Check:"
if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Airflow Web UI is accessible"
else
    echo "❌ Airflow Web UI is not accessible"
fi

echo ""
echo "📋 Quick Commands:"
echo "   Start:  ./start_airflow.sh"
echo "   Stop:   ./stop_airflow.sh"
echo "   Logs:   docker-compose -f docker-compose.airflow.yml logs"
