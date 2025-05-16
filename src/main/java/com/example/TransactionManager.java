package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vastdata.client.VastClient;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransaction;

/**
 * Manager for handling VastDB transactions.
 */
public class TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    private final VastClient client;
    private final TransactionFactory transactionFactory;

    public TransactionManager(VastClient client, TransactionFactory transactionFactory) {
        this.client = client;
        this.transactionFactory = transactionFactory;
    }

    /**
     * Starts a new transaction.
     */
    public SimpleVastTransaction startTransaction(StartTransactionContext context) {
        return transactionFactory.createTransaction(client, context);
    }

    /**
     * Commits a transaction.
     */
    public void commit(VastTransaction transaction) {
        try {
            client.commitTransaction(transaction);
        } catch (Exception e) {
            LOG.error("Failed to commit transaction", e);
            throw new RuntimeException("Failed to commit transaction", e);
        }
    }

    /**
     * Rolls back a transaction.
     */
    public void rollback(VastTransaction transaction) {
        try {
            LOG.info("Attempting to rollback transaction: {}", transaction);
            
            // Since the VastDB API might differ between versions, we'll handle rollback more carefully
            try {
                // Try to find and use the commitTransaction with a rollback flag
                java.lang.reflect.Method commitMethod = client.getClass().getMethod("commitTransaction", 
                        VastTransaction.class, boolean.class);
                LOG.info("Found commitTransaction method with rollback flag");
                commitMethod.invoke(client, transaction, false);
                LOG.info("Rollback successful using commitTransaction with flag");
                return;
            } catch (NoSuchMethodException e1) {
                LOG.warn("commitTransaction with rollback flag not found: {}", e1.getMessage());
            } catch (Exception e1) {
                LOG.warn("Failed to use commitTransaction with rollback flag: {}", e1.getMessage());
            }
            
            try {
                // Try to find the abort method using reflection
                java.lang.reflect.Method abortMethod = client.getClass().getMethod("abortTransaction", 
                        VastTransaction.class);
                LOG.info("Found abortTransaction method");
                abortMethod.invoke(client, transaction);
                LOG.info("Rollback successful using abortTransaction method");
                return;
            } catch (NoSuchMethodException e2) {
                LOG.warn("No abortTransaction method found: {}", e2.getMessage());
            } catch (Exception e2) {
                LOG.warn("Failed to use abortTransaction method: {}", e2.getMessage());
            }
            
            try {
                // Try to find the rollback method using reflection
                java.lang.reflect.Method rollbackMethod = client.getClass().getMethod("rollbackTransaction", 
                        VastTransaction.class);
                LOG.info("Found rollbackTransaction method");
                rollbackMethod.invoke(client, transaction);
                LOG.info("Rollback successful using rollbackTransaction method");
                return;
            } catch (NoSuchMethodException e3) {
                LOG.warn("No rollbackTransaction method found: {}", e3.getMessage());
            } catch (Exception e3) {
                LOG.warn("Failed to use rollbackTransaction method: {}", e3.getMessage());
            }
            
            LOG.error("Could not find a method to rollback transaction. Transaction may remain uncommitted.");
        } catch (Exception e) {
            LOG.error("Failed to rollback transaction", e);
            throw new RuntimeException("Failed to rollback transaction", e);
        }
    }

    /**
     * Executes work within a transaction and handles commit/rollback.
     */
    public void executeInTransaction(StartTransactionContext context, TransactionWork work) {
        SimpleVastTransaction transaction = startTransaction(context);
        try {
            work.execute(transaction);
            commit(transaction);
        } catch (Exception e) {
            LOG.error("Transaction failed, attempting rollback", e);
            rollback(transaction);
            throw new RuntimeException("Transaction failed", e);
        }
    }

    /**
     * Functional interface for work to be executed within a transaction.
     */
    @FunctionalInterface
    public interface TransactionWork {
        void execute(SimpleVastTransaction transaction) throws Exception;
    }
}