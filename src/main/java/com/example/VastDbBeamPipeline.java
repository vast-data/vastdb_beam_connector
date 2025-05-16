package com.example;

import com.example.config.VastDbPipelineOptions;
import com.example.io.VastDbIO;
import com.example.transforms.VastDbBatchWriter;
import com.example.utils.SchemaConverter;
import com.example.utils.VastDbTableUtils;
import com.example.utils.VastDbUtils;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for the VastDB Beam Pipeline MVP.
 */
public class VastDbBeamPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbBeamPipeline.class);

    public static void main(String[] args) {
        // Parse pipeline options
        VastDbPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(VastDbPipelineOptions.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // For demo purposes, we're using a simple schema with two string columns (x and y)
        // In a real application, you would retrieve or define the schema based on your needs
        org.apache.beam.sdk.schemas.Schema beamSchema = org.apache.beam.sdk.schemas.Schema.builder()
                .addNullableField("x", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                .addNullableField("y", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                .build();

        // Ensure the table exists
        ensureTableExists(options, beamSchema);

        // Read lines from the input file and convert them to Row objects
        pipeline.apply("ReadInputFile", TextIO.read().from(options.getInputFile()))
                .apply("ParseInputToRows", ParDo.of(new ParseTextToRow(beamSchema)))
                .apply("WriteToBatchWriter", ParDo.of(new VastDbBatchWriter(
                        options.getVastDbEndpoint().get(),
                        options.getVastDbSchemaName().get(),
                        options.getVastDbTableName().get(),
                        options.getBatchSize().get())));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    /**
     * DoFn to parse text input into Row objects.
     * This is a simplified example that assumes CSV input with two columns.
     */
    private static class ParseTextToRow extends DoFn<String, Row> {
        private final org.apache.beam.sdk.schemas.Schema schema;

        public ParseTextToRow(org.apache.beam.sdk.schemas.Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Row> out) {
            try {
                // Simple CSV parsing - for a real application, use a proper CSV parser
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    Row row = Row.withSchema(schema)
                            .withFieldValue("x", parts[0].trim())
                            .withFieldValue("y", parts[1].trim())
                            .build();
                    out.output(row);
                }
            } catch (Exception e) {
                LOG.error("Error parsing line: {}", line, e);
            }
        }
    }

    /**
     * Example alternative pipeline using the VastDbIO connector directly.
     */
    public static void runAlternativePipeline(VastDbPipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // Define schema
        org.apache.beam.sdk.schemas.Schema beamSchema = org.apache.beam.sdk.schemas.Schema.builder()
                .addNullableField("x", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                .addNullableField("y", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                .build();

        // Read and write using VastDbIO
        pipeline.apply("ReadInputFile", TextIO.read().from(options.getInputFile()))
                .apply("ParseInputToRows", ParDo.of(new ParseTextToRow(beamSchema)))
                .apply("WriteToVastDb", VastDbIO.write()
                        .withEndpoint(options.getVastDbEndpoint())
                        .withSchemaName(options.getVastDbSchemaName())
                        .withTableName(options.getVastDbTableName()));

        pipeline.run().waitUntilFinish();
    }

    /**
     * Utility method to ensure the target table exists in VastDB.
     */
    private static void ensureTableExists(VastDbPipelineOptions options, org.apache.beam.sdk.schemas.Schema beamSchema) {
        try {
            String endpoint = options.getVastDbEndpoint().get();
            String schemaName = options.getVastDbSchemaName().get();
            String tableName = options.getVastDbTableName().get();

            VastSdk vastSdk = VastDbUtils.createVastSdk(endpoint);
            
            // First ensure schema exists
            VastDbTableUtils.createSchemaIfNotExists(vastSdk, schemaName);
            
            // Convert Beam schema to Arrow schema
            Schema arrowSchema = SchemaConverter.beamSchemaToArrowSchemaWithBeamUtils(beamSchema);
            
            // Create table if not exists
            VastDbTableUtils.createTableIfNotExists(vastSdk, schemaName, tableName, arrowSchema);
            
            LOG.info("Successfully ensured table {}/{} exists with required schema", schemaName, tableName);
        } catch (Exception e) {
            LOG.error("Error ensuring table exists", e);
            throw new RuntimeException("Failed to ensure table exists", e);
        }
    }
}