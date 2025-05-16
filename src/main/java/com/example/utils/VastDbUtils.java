package com.example.utils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.VastSdkConfig;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;

/**
 * Utility class for VastDB operations.
 */
public class VastDbUtils {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbUtils.class);

    private VastDbUtils() {}

    /**
     * Creates a VastSdk instance.
     */
    public static VastSdk createVastSdk(String endpoint) {
        LOG.info("Creating VastSdk with endpoint: {}", endpoint);
        
        String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        if (awsAccessKeyId == null || awsSecretAccessKey == null) {
            LOG.error("AWS credentials not found in environment variables");
            throw new IllegalArgumentException("AWS credentials must be provided via environment variables");
        }

        URI uri = URI.create(endpoint);
        LOG.info("URI created: {}", uri);

        VastSdkConfig config = new VastSdkConfig(uri,
                uri.toString(),
                awsAccessKeyId,
                awsSecretAccessKey);
        LOG.info("VastSdkConfig created");

        try {
            HttpClient httpClient = new JettyHttpClient();
            LOG.info("HttpClient created");
            
            VastSdk sdk = new VastSdk(httpClient, config);
            LOG.info("VastSdk created successfully");
            return sdk;
        } catch (Exception e) {
            LOG.error("Error creating VastSdk", e);
            throw new RuntimeException("Failed to create VastSdk", e);
        }
    }

    /**
     * Converts a Beam Row to an Arrow VectorSchemaRoot.
     */
    public static VectorSchemaRoot convertRowToVectorSchemaRoot(Row row, Schema arrowSchema) {
        RootAllocator allocator = new RootAllocator();
        List<FieldVector> fieldVectors = new ArrayList<>();
        
        // Get Arrow schema fields without row_id (which is managed by VastDB)
        List<Field> fields = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            if (!field.getName().equals("$row_id")) {
                fields.add(field);
            }
        }
        
        // Create a vector for each field
        for (Field field : fields) {
            String fieldName = field.getName();
            
            // Basic implementation for String fields - expand this for other data types
            if (row.getSchema().getField(fieldName) != null) {
                // Create vector based on field type
                switch (field.getType().getTypeID()) {
                    case Utf8:
                        VarCharVector vector = new VarCharVector(fieldName, allocator);
                        vector.allocateNew(1);
                        String value = row.getString(fieldName);
                        if (value != null) {
                            vector.set(0, new Text(value));
                        }
                        vector.setValueCount(1);
                        fieldVectors.add(vector);
                        break;
                    // Add cases for other data types as needed
                    default:
                        LOG.warn("Unsupported field type: {}", field.getType().getTypeID());
                        break;
                }
            }
        }
        
        return new VectorSchemaRoot(arrowSchema, fieldVectors, 1);
    }

    /**
     * Converts a batch of Beam Rows to an Arrow VectorSchemaRoot.
     */
    public static VectorSchemaRoot convertRowBatchToVectorSchemaRoot(List<Row> rows, Schema arrowSchema) {
        if (rows.isEmpty()) {
            return null;
        }

        RootAllocator allocator = new RootAllocator();
        List<FieldVector> fieldVectors = new ArrayList<>();
        
        // Get Arrow schema fields without row_id
        List<Field> fields = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            if (!field.getName().equals("$row_id")) {
                fields.add(field);
            }
        }
        
        int rowCount = rows.size();
        
        // Create a vector for each field
        for (Field field : fields) {
            String fieldName = field.getName();
            
            // Basic implementation for String fields - expand this for other data types
            if (rows.get(0).getSchema().getField(fieldName) != null) {
                switch (field.getType().getTypeID()) {
                    case Utf8:
                        VarCharVector vector = new VarCharVector(fieldName, allocator);
                        vector.allocateNew(rowCount);
                        
                        for (int i = 0; i < rowCount; i++) {
                            String value = rows.get(i).getString(fieldName);
                            if (value != null) {
                                vector.set(i, new Text(value));
                            }
                        }
                        
                        vector.setValueCount(rowCount);
                        fieldVectors.add(vector);
                        break;
                    // Add cases for other data types as needed
                    default:
                        LOG.warn("Unsupported field type: {}", field.getType().getTypeID());
                        break;
                }
            }
        }
        
        return new VectorSchemaRoot(arrowSchema, fieldVectors, rowCount);
    }
}