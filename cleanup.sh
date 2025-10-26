#!/bin/bash

# Weather Observations and Forecasting - Cleanup Script
# This script cleans up all containers, images, and volumes

set -e

echo "🧹 Cleaning up Weather Observations and Forecasting Project..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Stop and remove main project containers
print_status "Stopping main project containers..."
docker-compose down -v 2>/dev/null || true

# Stop and remove Airflow containers
print_status "Stopping Airflow containers..."
cd airflow
docker-compose -f docker-compose.airflow.yml down -v 2>/dev/null || true
cd ..

# Remove project-specific images
print_status "Removing project-specific images..."
docker images --format "table {{.Repository}}:{{.Tag}}\t{{.ID}}" | grep -E "(weather-observations|airflow-airflow)" | awk '{print $2}' | xargs -r docker rmi -f 2>/dev/null || true

# Remove unused volumes
print_status "Removing unused volumes..."
docker volume prune -f

# Remove unused networks
print_status "Removing unused networks..."
docker network prune -f

print_success "Cleanup completed successfully!"
echo ""
echo "To start fresh, run: ./setup.sh"
