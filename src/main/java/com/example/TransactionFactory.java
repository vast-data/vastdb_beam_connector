package com.example;

import com.vastdata.client.VastClient;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;

/**
 * Factory for creating SimpleVastTransaction objects.
 */
public interface TransactionFactory {
    SimpleVastTransaction createTransaction(VastClient client, StartTransactionContext context);
}
