package com.flink.enrichment;

import com.flink.enrichment.utils.EnrichmentUtils;
import com.flink.enrichment.model.RawTransaction;
import com.flink.enrichment.model.CustomerState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
// import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EnrichmentProcessFunction extends KeyedProcessFunction<String, RawTransaction, String> {
	// Create a reusable Jackson ObjectMapper for converting enriched transactions into JSON.
    private static final ObjectMapper MAPPER = new ObjectMapper();

	// Read the high-amount transaction threshold from the environment or use the default value.
    private static final double HIGH_AMOUNT_THRESHOLD = Double.parseDouble(
	    System.getenv().getOrDefault("HIGH_AMOUNT_THRESHOLD", "100000000")
    );

	// Store customer-specific state that is maintained separately for each customerId key.
    private transient ValueState<CustomerState> customerState;

    // @Override
    // public void open(Configuration parameters) {
    //         //     public static void open(EnrichmentProcessFunction enrichmentProcessFunction, Configuration parameters) {
    //         ValueStateDescriptor<CustomerState> descriptor = new ValueStateDescriptor<>(
    //                         "customer-state",
    //                         Types.POJO(CustomerState.class));

    //         customerState = getRuntimeContext().getState(descriptor);
    //         // enrichmentProcessFunction.customerState = enrichmentProcessFunction.getRuntimeContext().getState(descriptor);
    // }
    

	// Initialize the customer state when the Flink operator starts.
    @Override
	public void open(OpenContext openContext) {
		// Define the name and data type of the customer state managed by Flink.
		ValueStateDescriptor<CustomerState> descriptor = new ValueStateDescriptor<>("customer-state", Types.POJO(CustomerState.class));

		// Register the customer state with Flink's runtime state backend.
		customerState = getRuntimeContext().getState(descriptor);
	}

	// Process each valid transaction received by this keyed process function.
    @Override
    public void processElement(RawTransaction transaction, Context context, Collector<String> out) throws Exception {
		// Retrieve the previous customer state associated with the current customerId.
		CustomerState state = customerState.value();

		// Create an empty customer state when this is the customer's first transaction.
        if (state == null) {
            state = new CustomerState();
        }

        // Determine enrichment features based on the current transaction and the customer's previous transaction history.
        boolean isNewDevice = EnrichmentUtils.isNewDevice(state.lastDeviceId, transaction.deviceId);
        boolean isNewLocation = EnrichmentUtils.isNewLocation(state.lastCity, transaction.city);
        boolean isNightTransaction = EnrichmentUtils.isNightTransaction(transaction.eventTimestamp);
        boolean isForeignTransaction = EnrichmentUtils.isForeignTransaction(transaction.country);
        boolean isHighAmount = EnrichmentUtils.isHighAmount(transaction.amount, HIGH_AMOUNT_THRESHOLD);
        String merchantRiskLevel = EnrichmentUtils.determineMerchantRisk(transaction);
        int accountAgeDays = EnrichmentUtils.determineAccountAge(transaction.customerId);
        String customerRiskLevel = EnrichmentUtils.determineCustomerRisk(accountAgeDays);

        // Create a new JSON object that will contain the enriched transaction data.
		ObjectNode enriched = MAPPER.createObjectNode();

		// Fill new JSON object with the enriched transaction data.
        enriched.put("transaction_id", transaction.transactionId);
        enriched.put("customer_id", transaction.customerId);
        enriched.put("account_id", transaction.accountId);
        enriched.put("transaction_type", transaction.transactionType);
        enriched.put("amount", transaction.amount);
        enriched.put("currency", transaction.currency);
        enriched.put("merchant_id", transaction.merchantId);
        enriched.put("merchant_name", transaction.merchantName);
        enriched.put("merchant_category", transaction.merchantCategory);
        enriched.put("payment_method", transaction.paymentMethod);
        enriched.put("device_id", transaction.deviceId);
        enriched.put("ip_address", transaction.ipAddress);
        enriched.put("city", transaction.city);
        enriched.put("country", transaction.country);
        enriched.put("latitude", transaction.latitude);
        enriched.put("longitude", transaction.longitude);
        enriched.put("channel", transaction.channel);
        enriched.put("account_age_days", accountAgeDays);
        enriched.put("customer_risk_level", customerRiskLevel);
        enriched.put("merchant_risk_level", merchantRiskLevel);
        enriched.put("is_new_device", isNewDevice);
        enriched.put("is_new_location", isNewLocation);
        enriched.put("is_foreign_transaction", isForeignTransaction);
        enriched.put("is_night_transaction", isNightTransaction);
        enriched.put("is_high_amount", isHighAmount);

		// Convert event timestamp
        enriched.put("event_date", EnrichmentUtils.eventDate(transaction.eventTimestamp));
        enriched.put("event_hour", EnrichmentUtils.eventHour(transaction.eventTimestamp));
		enriched.put("timestamp", java.time.Instant.ofEpochMilli(transaction.eventTimestamp).toString());

		// Record the current processing time when the transaction is enriched.
        enriched.put("processing_time", java.time.Instant.now().toString());

		// Update the customer's last data
        state.lastDeviceId = transaction.deviceId;
        state.lastCity = transaction.city;
        state.lastTransactionTimestamp = transaction.eventTimestamp;
        state.lastAmount = transaction.amount;

        // Persist the updated customer state so it can be used by the next transaction for this customer.
        customerState.update(state);

        // Serialize the enriched JSON object into a string and emit it to the downstream Kafka sink.
        out.collect(MAPPER.writeValueAsString(enriched));
    }
}