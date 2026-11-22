// State ini menyimpan historical behavior customer seperti yang didefinisikan di summary.md.
package com.flink.engine.model;

import java.io.Serializable;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the state object used to store historical customer behavior.
public class CustomerBehaviorState implements Serializable {
    // Define the fields to store the customer's previous transaction information.
    public Long lastTransactionTime;
    public String lastTransactionCity;
    public String lastDevice;
    public Double lastAmount;
    public Long transactionCount;
    public Double totalTransactionAmount;

    // Initialize the customer behavior state with default counter values.
    public CustomerBehaviorState() {
        // Initialize the transaction count to zero for a new customer state.
        this.transactionCount = 0L;

        // Initialize the total transaction amount to zero for a new customer state.
        this.totalTransactionAmount = 0.0;
    }
}