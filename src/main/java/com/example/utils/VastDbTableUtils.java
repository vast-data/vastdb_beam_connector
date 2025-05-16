package com.example.utils;

import com.example.TransactionManager;
import com.example.SimpleTransactionFactory;
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.DropTableContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for VastDB table operations.
 */
public class VastDbTableUtils {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbTableUtils.class);

    private VastDbTableUtils() {}

    /**
     * Creates a table in VastDB if it doesn't exist.
     */
    public static void createTableIfNotExists(VastSdk vastSdk, String schemaName, String tableName, Schema schema) {
        VastClient client = vastSdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        
        try {
            // Start a transaction
            SimpleVastTransaction tx = transactionsManager
                    .startTransaction(new StartTransactionContext(false, false));
            
            try {
                // Check if table exists
                Table table = vastSdk.getTable(schemaName, tableName);
                table.loadSchema();
                LOG.info("Table {}/{} already exists", schemaName, tableName);
            } catch (Exception e) {
                LOG.info("Creating table {}/{}", schemaName, tableName);
                
                // Add row_id field if not present
                List<Field> fieldsWithRowId = new ArrayList<>(schema.getFields());
                
                // Check if row_id is already in the schema
                boolean hasRowId = fieldsWithRowId.stream()
                        .anyMatch(field -> field.getName().equals("$row_id"));
                
                if (!hasRowId) {
                    fieldsWithRowId.add(0, ArrowSchemaUtils.VASTDB_ROW_ID_FIELD);
                }
                
                // Create the table
                client.createTable(tx, new CreateTableContext(schemaName, tableName, fieldsWithRowId, null, null));
            }
            
            // Commit the transaction
            transactionsManager.commit(tx);
        } catch (Exception e) {
            LOG.error("Error creating table {}/{}", schemaName, tableName, e);
            throw new RuntimeException("Failed to create table", e);
        }
    }

    /**
     * Creates a schema in VastDB if it doesn't exist.
     */
    public static void createSchemaIfNotExists(VastSdk vastSdk, String schemaName) {
        VastClient client = vastSdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        
        try {
            // Start a transaction
            SimpleVastTransaction tx = transactionsManager
                    .startTransaction(new StartTransactionContext(false, false));
            
            if (!client.schemaExists(tx, schemaName)) {
                LOG.info("Creating schema {}", schemaName);
                client.createSchema(tx, schemaName,
                        new VastMetadataUtils().getPropertiesString(Collections.emptyMap()));
            } else {
                LOG.info("Schema {} already exists", schemaName);
            }
            
            // Commit the transaction
            transactionsManager.commit(tx);
        } catch (Exception e) {
            LOG.error("Error creating schema {}", schemaName, e);
            throw new RuntimeException("Failed to create schema", e);
        }
    }

    /**
     * Drops a table from VastDB if it exists.
     */
    public static void dropTableIfExists(VastSdk vastSdk, String schemaName, String tableName) {
        VastClient client = vastSdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        
        try {
            // Start a transaction
            SimpleVastTransaction tx = transactionsManager
                    .startTransaction(new StartTransactionContext(false, false));
            
            try {
                // Check if table exists
                Table table = vastSdk.getTable(schemaName, tableName);
                table.loadSchema();
                
                // Drop the table
                LOG.info("Dropping table {}/{}", schemaName, tableName);
                client.dropTable(tx, new DropTableContext(schemaName, tableName));
            } catch (Exception e) {
                LOG.info("Table {}/{} does not exist", schemaName, tableName);
            }
            
            // Commit the transaction
            transactionsManager.commit(tx);
        } catch (Exception e) {
            LOG.error("Error dropping table {}/{}", schemaName, tableName, e);
            throw new RuntimeException("Failed to drop table", e);
        }
    }

    /**
     * Lists all tables in a schema.
     */
    public static List<String> listTables(VastSdk vastSdk, String schemaName) throws VastException {
        VastClient client = vastSdk.getVastClient();
        TransactionManager transactionsManager = new TransactionManager(client, new SimpleTransactionFactory());
        
        // Start a transaction
        SimpleVastTransaction tx = transactionsManager
                .startTransaction(new StartTransactionContext(false, false));
        
        try {
            // List tables
            Stream<String> tableStream = client.listTables(tx, schemaName, 1000);
            List<String> tables = tableStream.collect(Collectors.toList());
            
            // Commit the transaction
            transactionsManager.commit(tx);
            
            return tables;
        } catch (Exception e) {
            // Rollback the transaction
            transactionsManager.rollback(tx);
            
            LOG.error("Error listing tables in schema {}", schemaName, e);
            throw new RuntimeException("Failed to list tables", e);
        }
    }
}
