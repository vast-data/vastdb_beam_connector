package com.example.transforms;

import com.example.utils.VastDbUtils;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A DoFn that batches rows and writes them to VastDB.
 */
public class VastDbBatchWriter extends DoFn<Row, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbBatchWriter.class);

    private final String endpoint;
    private final String schemaName;
    private final String tableName;
    private final int batchSize;
    
    private transient VastSdk vastSdk;
    private transient Table table;
    private transient List<Row> batch;

    public VastDbBatchWriter(String endpoint, String schemaName, String tableName, int batchSize) {
        this.endpoint = endpoint;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    @Setup
    public void setup() {
        LOG.info("Setting up VastDB connection to endpoint: {}", endpoint);
        vastSdk = VastDbUtils.createVastSdk(endpoint);
        
        // Initialize batch
        batch = new ArrayList<>(batchSize);
        
        // Ensure table exists
        try {
            table = vastSdk.getTable(schemaName, tableName);
            table.loadSchema();
            LOG.info("Successfully connected to table {}/{}", schemaName, tableName);
        } catch (Exception e) {
            LOG.error("Failed to connect to VastDB table {}/{}", schemaName, tableName, e);
            throw new RuntimeException("Failed to connect to VastDB table", e);
        }
    }

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<Void> out) {
        batch.add(row);
        
        // If batch is full, flush to VastDB
        if (batch.size() >= batchSize) {
            flushBatch();
        }
    }

    @FinishBundle
    public void finishBundle() {
        // Flush any remaining rows
        if (!batch.isEmpty()) {
            flushBatch();
        }
    }

    private void flushBatch() {
        try {
            LOG.info("Writing batch of {} rows to VastDB", batch.size());
            
            // Convert batch of rows to VectorSchemaRoot
            VectorSchemaRoot vectorSchemaRoot = VastDbUtils.convertRowBatchToVectorSchemaRoot(batch, table.getSchema());
            
            if (vectorSchemaRoot != null) {
                // Write to VastDB
                table.put(vectorSchemaRoot);
                
                // Clean up resources
                vectorSchemaRoot.close();
            }
            
            // Clear batch
            batch.clear();
        } catch (Exception e) {
            LOG.error("Error writing batch to VastDB", e);
            throw new RuntimeException("Failed to write batch to VastDB", e);
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