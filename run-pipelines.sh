#!/bin/bash

# Check if the required environment variables are set
if [ -z "$ENDPOINT" ]; then
  echo "Error: ENDPOINT environment variable is not set"
  echo "Usage: export ENDPOINT=https://your-vastdb-endpoint"
  exit 1
fi

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "Error: AWS_ACCESS_KEY_ID environment variable is not set"
  exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "Error: AWS_SECRET_ACCESS_KEY environment variable is not set"
  exit 1
fi

# Default values
SCHEMA_NAME=${SCHEMA_NAME:-"vastdb/s2"}
TABLE_NAME=${TABLE_NAME:-"beam_test_table"}
INPUT_FILE=${INPUT_FILE:-"sample-data.csv"}
BATCH_SIZE=${BATCH_SIZE:-100}

# Build the project if not already built
if [ ! -f "target/beam-vastdb-writer-1.0.0-jar-with-dependencies.jar" ]; then
  echo "Building the project..."
  mvn clean package
fi

# Run the pipeline
echo "Running the VastDB Beam Pipeline..."
echo "Endpoint: $ENDPOINT"
echo "Schema Name: $SCHEMA_NAME"
echo "Table Name: $TABLE_NAME"
echo "Input File: $INPUT_FILE"
echo "Batch Size: $BATCH_SIZE"

# Define JVM arguments for Apache Arrow on Java 16+
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"

java $JAVA_OPTS -jar target/beam-vastdb-writer-1.0.0-jar-with-dependencies.jar \
  --vastDbEndpoint="$ENDPOINT" \
  --vastDbSchemaName="$SCHEMA_NAME" \
  --vastDbTableName="$TABLE_NAME" \
  --inputFile="$INPUT_FILE" \
  --batchSize="$BATCH_SIZE"

echo "Pipeline execution completed."