#!/bin/bash

# Script to build Flink job JAR file
# This script should be run before building the Docker image

set -e

echo "Building Flink job JAR..."

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Build the project
echo "Running Maven build..."
mvn clean package -DskipTests

# Check if JAR was created
JAR_PATH="target/flink-kafka-paimon-clickhouse-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file was not created at $JAR_PATH"
    exit 1
fi

echo "Build successful! JAR file created at: $JAR_PATH"
echo "You can now build the Docker image."

