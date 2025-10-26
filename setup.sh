#!/bin/bash

# Weather Observations and Forecasting - Setup Script
# This script sets up the complete environment for running the project

set -e  # Exit on any error

echo "🚀 Setting up Weather Observations and Forecasting Project..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is installed and running
check_docker() {
    print_status "Checking Docker installation..."
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    print_success "Docker is installed and running"
}

# Check if Docker Compose is available
check_docker_compose() {
    print_status "Checking Docker Compose..."
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    print_success "Docker Compose is available"
}

# Create .env file if it doesn't exist
create_env_file() {
    print_status "Setting up environment configuration..."
    
    if [ ! -f .env ]; then
        print_status "Creating .env file with default values..."
        cat > .env << EOF
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
MAIN_DB=weatherdb

# Schema Configuration
RAW_SCHEMA=raw
STAGING_SCHEMA=staging
DIMENSIONS_SCHEMA=dimensions
FACT_SCHEMA=fact
BUSINESS_MART_SCHEMA=business_mart

# BrightSky API Configuration
BRIGHTSKY_BASE=https://api.brightsky.dev

# Ingestion Configuration
HTTP_CONCURRENCY=10
FORECAST_DAYS_BY=3

# Airflow Configuration
FERNET_KEY=$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=airflow
EOF
        print_success "Created .env file with default configuration"
    else
        print_warning ".env file already exists, skipping creation"
    fi
}

# Start the main PostgreSQL database
start_main_database() {
    print_status "Starting main PostgreSQL database..."
    docker-compose up -d postgres
    
    # Wait for database to be ready
    print_status "Waiting for database to be ready..."
    sleep 10
    
    # Check if database is healthy
    if docker-compose ps postgres | grep -q "healthy"; then
        print_success "Main PostgreSQL database is running and healthy"
    else
        print_error "Failed to start PostgreSQL database"
        exit 1
    fi
}

# Start Airflow
start_airflow() {
    print_status "Starting Airflow services..."
    
    cd airflow
    docker-compose -f docker-compose.airflow.yml up -d
    
    # Wait for Airflow to be ready
    print_status "Waiting for Airflow to initialize..."
    sleep 30
    
    # Check if Airflow is accessible
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health | grep -q "200"; then
        print_success "Airflow is running and accessible"
    else
        print_warning "Airflow may still be starting up. Please wait a few more minutes."
    fi
    
    cd ..
}

# Display final information
show_final_info() {
    echo ""
    echo "🎉 Setup completed successfully!"
    echo ""
    echo "📋 Access Information:"
    echo "  • Airflow UI: http://localhost:8080"
    echo "  • Airflow Credentials: admin / admin"
    echo "  • PostgreSQL (Main): localhost:5432"
    echo "  • PostgreSQL (Airflow): localhost:5433"
    echo ""
    echo "🔧 Useful Commands:"
    echo "  • View logs: docker-compose logs -f"
    echo "  • Stop services: docker-compose down"
    echo "  • Restart Airflow: cd airflow && docker-compose -f docker-compose.airflow.yml restart"
    echo ""
    echo "📚 Next Steps:"
    echo "  1. Open http://localhost:8080 in your browser"
    echo "  2. Login with admin/admin"
    echo "  3. Enable and run the DAGs"
    echo ""
}

# Main execution
main() {
    echo "=========================================="
    echo "Weather Observations and Forecasting Setup"
    echo "=========================================="
    echo ""
    
    check_docker
    check_docker_compose
    create_env_file
    start_main_database
    start_airflow
    show_final_info
}

# Run main function
main "$@"