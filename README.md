# Apache Beam VastDB Writer

This project provides an MVP (Minimum Viable Product) implementation of an Apache Beam Writer for VastDB, allowing you to integrate Apache Beam data pipelines with VastDB storage.

## Overview

The Apache Beam VastDB Writer enables you to write data from Apache Beam pipelines directly to VastDB tables. It uses Apache Arrow as the intermediate representation format to efficiently transfer data between Beam and VastDB.

## Features

- Write Beam PCollection<Row> data to VastDB tables
- Automatic schema conversion between Beam and Arrow
- Support for batched writes to improve performance
- Custom IO connector (VastDbIO) for simplified pipeline integration
- Automatic table and schema creation

## Prerequisites

- Java 17+
- Maven
- VastDB credentials (AWS access keys)

## Getting Started

### Build the Project

```bash
mvn clean package
```

### Run the Pipeline

To run the pipeline, you need to provide the required options:

```bash
export ENDPOINT="https://your-vastdb-endpoint"
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"

java -jar target/beam-vastdb-writer-1.0.0-jar-with-dependencies.jar \
    --vastDbEndpoint="$ENDPOINT" \
    --vastDbSchemaName="vastdb/s2" \
    --vastDbTableName="my_table" \
    --inputFile="/path/to/your/input.csv" \
    --batchSize=100
```

## Usage Examples

### Basic Usage

```java
// Define your pipeline options
VastDbPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(VastDbPipelineOptions.class);

// Create the pipeline
Pipeline pipeline = Pipeline.create(options);

// Define your data schema
Schema beamSchema = Schema.builder()
        .addNullableField("x", Schema.FieldType.STRING)
        .addNullableField("y", Schema.FieldType.STRING)
        .build();

// Read data and write to VastDB
pipeline.apply("ReadData", ReadFromSource.create(...))
        .apply("WriteToVastDb", VastDbIO.write()
                .withEndpoint(options.getVastDbEndpoint())
                .withSchemaName(options.getVastDbSchemaName())
                .withTableName(options.getVastDbTableName()));

// Run the pipeline
pipeline.run();
```

### Batch Writing

For better performance with large datasets, use the batch writer:

```java
pipeline.apply("ReadData", ReadFromSource.create(...))
        .apply("WriteWithBatchWriter", ParDo.of(new VastDbBatchWriter(
                endpoint, 
                schemaName, 
                tableName, 
                batchSize)));
```

## Architecture

The project consists of the following main components:

1. **VastDbIO**: An IO connector for integrating VastDB with Apache Beam
2. **VastDbBatchWriter**: A DoFn for efficient batch writing to VastDB
3. **SchemaConverter**: Utilities for converting between Beam and Arrow schemas
4. **VastDbUtils**: Helper methods for VastDB operations
5. **VastDbTableUtils**: Utilities for table management operations

## Technical Details

### Data Flow

```
Apache Beam Row → SchemaConverter → Apache Arrow VectorSchemaRoot → VastDB Table
```

### Schema Handling

The project automatically handles schema conversion between Beam's Row schema and Arrow's schema required by VastDB. It handles nested field types and supports the core data types.

## Limitations and Future Improvements

This MVP implementation has the following limitations:

1. Support for a limited set of data types (mainly String, Integer, etc.)
2. Basic error handling and recovery
3. Limited performance optimizations

Future improvements could include:

1. Support for all Apache Arrow data types
2. Advanced error handling with dead-letter queues
3. Parallel writing to VastDB
4. Reading from VastDB into Beam pipelines
5. Dynamic schema discovery

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0.
