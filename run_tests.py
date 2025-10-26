#!/usr/bin/env python3
"""
Test runner for the weather data ingestion pipeline.
Runs Python unit tests from the ingestion_tests/ directory.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))


def run_tests(test_type="all", verbose=False, coverage=False):
    """
    Run tests with specified options.
    
    Args:
        test_type (str): Type of tests to run ('all', 'unit', 'integration', 'database')
        verbose (bool): Enable verbose output
        coverage (bool): Enable coverage reporting
    """
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add test directory
    cmd.append("ingestion_tests/")
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    
    # Add coverage
    if coverage:
        cmd.append("--cov=src")
        cmd.append("--cov=utils")
        cmd.append("--cov-report=html")
        cmd.append("--cov-report=term")
    
    # Add test type filters
    if test_type == "unit":
        cmd.append("-m")
        cmd.append("unit")
    elif test_type == "integration":
        cmd.append("-m")
        cmd.append("integration")
    elif test_type == "database":
        cmd.append("-m")
        cmd.append("database")
    elif test_type == "slow":
        cmd.append("-m")
        cmd.append("slow")
    
    # Add other options
    cmd.extend([
        "--tb=short",
        "--strict-markers",
        "--disable-warnings"
    ])
    
    print(f"Running tests: {' '.join(cmd)}")
    print("=" * 50)
    
    # Run tests
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 50)
        print("✅ All tests passed!")
        return True
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 50)
        print(f"❌ Tests failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False


def main():
    """Main function for test runner."""
    parser = argparse.ArgumentParser(description="Run weather data pipeline tests")
    parser.add_argument(
        "--type", 
        choices=["all", "unit", "integration", "database", "slow"],
        default="all",
        help="Type of tests to run"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Enable coverage reporting"
    )
    parser.add_argument(
        "--html-report",
        action="store_true",
        help="Generate HTML test report"
    )
    
    args = parser.parse_args()
    
    # Add HTML report if requested
    if args.html_report:
        cmd = ["python", "-m", "pytest", "ingestion_tests/", "--html=test_report.html", "--self-contained-html"]
        if args.verbose:
            cmd.append("-v")
        if args.coverage:
            cmd.extend(["--cov=src", "--cov=utils", "--cov-report=html"])
        
        print(f"Running tests with HTML report: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
            print("✅ HTML test report generated: test_report.html")
        except subprocess.CalledProcessError as e:
            print(f"❌ Tests failed with exit code {e.returncode}")
            return False
    
    # Run regular tests
    success = run_tests(
        test_type=args.type,
        verbose=args.verbose,
        coverage=args.coverage
    )
    
    if success:
        print("\n🎉 Test run completed successfully!")
        if args.coverage:
            print("📊 Coverage report generated in htmlcov/")
        if args.html_report:
            print("📄 HTML test report generated: test_report.html")
    else:
        print("\n💥 Test run failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()

