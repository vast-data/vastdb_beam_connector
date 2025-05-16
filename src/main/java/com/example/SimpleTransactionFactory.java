package com.example;

import java.util.Collection;

import com.google.common.collect.Multimap;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastResponse;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;

import io.airlift.http.client.HeaderName;

/**
 * Factory for creating SimpleVastTransaction objects.
 */
public class SimpleTransactionFactory implements TransactionFactory {
    @Override
    public SimpleVastTransaction createTransaction(VastClient client, StartTransactionContext context) {
        try {
            // Directly call the startTransaction method
            Object response = client.startTransaction(context);
            
            if (response instanceof SimpleVastTransaction) {
                return (SimpleVastTransaction) response;
            } else if (response instanceof VastResponse) {
                VastResponse vastResponse = (VastResponse) response;
                
                // Extract the transaction ID from the headers
                String txId = null;
                Multimap<HeaderName, String> headers = vastResponse.getHeaders();

                if (headers != null) {
                    // Find the tabular-txid header
                    for (HeaderName headerName : headers.keySet()) {
                        String headerNameStr = headerName.toString();
                        if (headerNameStr.equalsIgnoreCase("tabular-txid")) {
                            Collection<String> values = headers.get(headerName);
                            if (values != null && !values.isEmpty()) {
                                txId = values.iterator().next();
                                System.out.println("Found transaction ID in headers: " + txId);
                                break;
                            }
                        }
                    }
                }
                
                if (txId == null) {
                    System.out.println("No transaction ID found in headers");
                    System.out.println("Headers: " + vastResponse.getHeaders());
                    
                    // Fall back to extracting from toString
                    String responseString = vastResponse.toString();
                    if (responseString.contains("tabular-txid=[")) {
                        int start = responseString.indexOf("tabular-txid=[") + "tabular-txid=[".length();
                        int end = responseString.indexOf("]", start);
                        if (end > start) {
                            txId = responseString.substring(start, end);
                            System.out.println("Extracted transaction ID from toString: " + txId);
                        }
                    }
                }
                
                if (txId != null) {
                    // Create a new SimpleVastTransaction using the constructor with long parameter
                    try {
                        java.lang.reflect.Constructor<SimpleVastTransaction> constructor = 
                            SimpleVastTransaction.class.getConstructor(long.class, boolean.class, boolean.class);
                        return constructor.newInstance(Long.parseLong(txId), false, false);
                    } catch (Exception e) {
                        System.out.println("Error creating SimpleVastTransaction with long constructor: " + e.getMessage());
                        
                        // Try the default constructor and set the transaction ID using reflection
                        try {
                            SimpleVastTransaction tx = SimpleVastTransaction.class.getDeclaredConstructor().newInstance();
                            
                            // Try to set the transaction ID using reflection
                            try {
                                java.lang.reflect.Field txIdField = SimpleVastTransaction.class.getDeclaredField("id");
                                txIdField.setAccessible(true);
                                txIdField.set(tx, Long.parseLong(txId));
                                System.out.println("Set transaction ID using reflection");
                                return tx;
                            } catch (Exception e2) {
                                System.out.println("Error setting transaction ID using reflection: " + e2.getMessage());
                            }
                            
                            // Try to find a setId method
                            try {
                                java.lang.reflect.Method setIdMethod = SimpleVastTransaction.class.getDeclaredMethod("setId", long.class);
                                setIdMethod.setAccessible(true);
                                setIdMethod.invoke(tx, Long.parseLong(txId));
                                System.out.println("Set transaction ID using setId method");
                                return tx;
                            } catch (Exception e3) {
                                System.out.println("Error setting transaction ID using setId method: " + e3.getMessage());
                            }
                        } catch (Exception e4) {
                            System.out.println("Error creating SimpleVastTransaction with default constructor: " + e4.getMessage());
                        }
                    }
                }
                
                throw new RuntimeException("Could not create a SimpleVastTransaction from the response: " + vastResponse);
            } else {
                throw new RuntimeException("Unexpected response type: " + response.getClass().getName());
            }
        } catch (Exception e) {
            throw new RuntimeException("Transaction creation failed", e);
        }
    }
}