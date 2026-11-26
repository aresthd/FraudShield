// State ini menyimpan historical behavior customer seperti yang didefinisikan di summary.md.
package com.flink.engine.model;

import java.io.Serializable;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the state object used to store historical customer behavior.
public class CustomerBehaviorState implements Serializable {
    public Long lastTransactionTime;            // Store the timestamp of the customer's previous transaction.
    public String lastTransactionCity;          // Store the city of the customer's previous transaction.
    public String lastDevice;                   // Store the device used in the customer's previous transaction.
    public Double lastAmount;                   // Store the amount of the customer's previous transaction.
    public Long transactionCount;               // Store the total number of transactions made by the customer.
    public Double totalTransactionAmount;       // Store the total transaction amount made by the customer.

    // Initialize the customer behavior state with default counter values.
    public CustomerBehaviorState() {
        // Initialize the transaction count to zero for a new customer state.
        this.transactionCount = 0L;

        // Initialize the total transaction amount to zero for a new customer state.
        this.totalTransactionAmount = 0.0;
    }
}