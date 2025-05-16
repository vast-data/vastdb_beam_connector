package com.example.io;

import com.example.utils.VastDbUtils;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * An IO connector for writing data to VastDB from Apache Beam pipelines.
 */
public class VastDbIO {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbIO.class);

    private VastDbIO() {}

    /**
     * Creates a write transform for VastDB.
     */
    public static Write<Row> write() {
        return new Write<>(new VastDbWrite());
    }

    /**
     * A {@link PTransform} that writes to VastDB.
     */
    private static class VastDbWrite extends PTransform<PCollection<Row>, PDone> {
        
        @Override
        public PDone expand(PCollection<Row> input) {
            input.apply(ParDo.of(new VastDbWriter(null, null, null)));
            return PDone.in(input.getPipeline());
        }
    }

    /**
     * Configuration for writing to VastDB.
     */
    public static class Write<T> extends PTransform<PCollection<T>, PDone> {
        private final VastDbWrite spec;
        private ValueProvider<String> endpoint;
        private ValueProvider<String> schemaName; 
        private ValueProvider<String> tableName;
        private SerializableFunction<T, Row> formatFunction;

        private Write(VastDbWrite spec) {
            this.spec = spec;
        }

        /**
         * Sets the VastDB endpoint.
         */
        public Write<T> withEndpoint(ValueProvider<String> endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Sets the schema name.
         */
        public Write<T> withSchemaName(ValueProvider<String> schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        /**
         * Sets the table name.
         */
        public Write<T> withTableName(ValueProvider<String> tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Sets a function to format the input elements into {@link Row} objects.
         */
        public Write<Row> withFormatFunction(SerializableFunction<T, Row> formatFunction) {
            this.formatFunction = formatFunction;
            return (Write<Row>) this;
        }

        @Override
        public PDone expand(PCollection<T> input) {
            PCollection<Row> rows;
            if (formatFunction != null) {
                rows = input.apply("FormatRows", org.apache.beam.sdk.transforms.MapElements
                        .into(org.apache.beam.sdk.values.TypeDescriptors.rows())
                        .via(formatFunction));
            } else {
                rows = (PCollection<Row>) input;
            }

            rows.apply("WriteToVastDb", ParDo.of(new VastDbWriter(endpoint, schemaName, tableName)));
            return PDone.in(input.getPipeline());
        }
    }

    /**
     * A DoFn that writes rows to VastDB.
     */
    private static class VastDbWriter extends DoFn<Row, Void> implements Serializable {
        private final ValueProvider<String> endpoint;
        private final ValueProvider<String> schemaName;
        private final ValueProvider<String> tableName;
        private transient VastSdk vastSdk;
        private transient Table table;

        public VastDbWriter(ValueProvider<String> endpoint, ValueProvider<String> schemaName, ValueProvider<String> tableName) {
            this.endpoint = endpoint;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Setup
        public void setup() {
            LOG.info("Setting up VastDB connection to endpoint: {}", endpoint.get());
            vastSdk = VastDbUtils.createVastSdk(endpoint.get());
            
            // Ensure table exists
            try {
                table = vastSdk.getTable(schemaName.get(), tableName.get());
                table.loadSchema();
                LOG.info("Successfully connected to table {}/{}", schemaName.get(), tableName.get());
            } catch (Exception e) {
                LOG.error("Failed to connect to VastDB table {}/{}", schemaName.get(), tableName.get(), e);
                throw new RuntimeException("Failed to connect to VastDB table", e);
            }
        }

        @ProcessElement
        public void processElement(@Element Row row, OutputReceiver<Void> out) {
            try {
                // Convert Beam Row to Arrow VectorSchemaRoot
                VectorSchemaRoot vectorSchemaRoot = VastDbUtils.convertRowToVectorSchemaRoot(row, table.getSchema());
                
                // Write to VastDB
                table.put(vectorSchemaRoot);
                
                // Clean up resources
                vectorSchemaRoot.close();
            } catch (Exception e) {
                LOG.error("Error writing row to VastDB: {}", row, e);
                throw new RuntimeException("Failed to write to VastDB", e);
            }
        }

        @Teardown
        public void teardown() {
            LOG.info("Tearing down VastDB connection");
            if (vastSdk != null) {
                // Any cleanup needed
            }
        }
    }
}