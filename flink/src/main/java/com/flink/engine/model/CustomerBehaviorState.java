// State ini menyimpan historical behavior customer seperti yang didefinisikan di summary.md.
package com.flink.engine.model;

import java.io.Serializable;

public class CustomerBehaviorState implements Serializable {
    public Long lastTransactionTime;
    public String lastTransactionCity;
    public String lastDevice;
    public Double lastAmount;
    public Long transactionCount;
    public Double totalTransactionAmount;

    public CustomerBehaviorState() {
        this.transactionCount = 0L;
        this.totalTransactionAmount = 0.0;
    }
}