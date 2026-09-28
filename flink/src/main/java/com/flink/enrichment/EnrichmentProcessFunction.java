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

        // Determine whether the transaction uses a device that has not been seen previously by the customer.
        boolean isNewDevice = EnrichmentUtils.isNewDevice(state.lastDeviceId, transaction.deviceId);

        // Determine whether the transaction originates from a location that has not been seen previously by the customer.
        boolean isNewLocation = EnrichmentUtils.isNewLocation(state.lastCity, transaction.city);

        // Determine whether the transaction occurred during the configured night-time period.
        boolean isNightTransaction = EnrichmentUtils.isNightTransaction(transaction.eventTimestamp);

        // Determine whether the transaction occurred in a foreign country.
        boolean isForeignTransaction = EnrichmentUtils.isForeignTransaction(transaction.country);

        // Determine whether the transaction amount exceeds the configured high-amount threshold.
        boolean isHighAmount = EnrichmentUtils.isHighAmount(transaction.amount, HIGH_AMOUNT_THRESHOLD);

        // Determine the risk level associated with the transaction merchant.
        String merchantRiskLevel = EnrichmentUtils.determineMerchantRisk(transaction);

        // Determine the customer's account age in days.
        int accountAgeDays = EnrichmentUtils.determineAccountAge(transaction.customerId);

        // Determine the customer's risk level based on the account age.
        String customerRiskLevel = EnrichmentUtils.determineCustomerRisk(accountAgeDays);

        // Create a new JSON object that will contain the enriched transaction data.
		ObjectNode enriched = MAPPER.createObjectNode();

		// Fill new JSON object with the enriched transaction data.
        enriched.put("transaction_id", transaction.transactionId);			// Add the original transaction ID to the enriched transaction.
        enriched.put("customer_id", transaction.customerId);				// Add the original customer ID to the enriched transaction.
        enriched.put("account_id", transaction.accountId);					// Add the original account ID to the enriched transaction.
        enriched.put("transaction_type", transaction.transactionType);		// Add the original transaction type to the enriched transaction.
        enriched.put("amount", transaction.amount);							// Add the original transaction amount to the enriched transaction.
        enriched.put("currency", transaction.currency);						// Add the original transaction currency to the enriched transaction.
        enriched.put("merchant_id", transaction.merchantId);				// Add the original merchant ID to the enriched transaction.
        enriched.put("merchant_name", transaction.merchantName);			// Add the original merchant name to the enriched transaction.
        enriched.put("merchant_category", transaction.merchantCategory);	// Add the original merchant category to the enriched transaction.
        enriched.put("payment_method", transaction.paymentMethod);			// Add the original payment method to the enriched transaction.
        enriched.put("device_id", transaction.deviceId);					// Add the original device ID to the enriched transaction.
        enriched.put("ip_address", transaction.ipAddress);					// Add the original IP address to the enriched transaction.
        enriched.put("city", transaction.city);								// Add the original transaction city to the enriched transaction.
        enriched.put("country", transaction.country);						// Add the original transaction country to the enriched transaction.
        enriched.put("latitude", transaction.latitude);						// Add the original latitude coordinate to the enriched transaction.
        enriched.put("longitude", transaction.longitude);					// Add the original longitude coordinate to the enriched transaction.
        enriched.put("channel", transaction.channel);						// Add the original transaction channel to the enriched transaction.
        enriched.put("account_age_days", accountAgeDays);					// Add the calculated customer account age to the enriched transaction.
        enriched.put("customer_risk_level", customerRiskLevel);				// Add the calculated customer risk level to the enriched transaction.
        enriched.put("merchant_risk_level", merchantRiskLevel);				// Add the calculated merchant risk level to the enriched transaction.
        enriched.put("is_new_device", isNewDevice);							// Add whether the transaction uses a new device for this customer.
        enriched.put("is_new_location", isNewLocation);						// Add whether the transaction occurs in a new location for this customer.
        enriched.put("is_foreign_transaction", isForeignTransaction);		// Add whether the transaction occurs in a foreign country.
        enriched.put("is_night_transaction", isNightTransaction);			// Add whether the transaction occurs during night-time.
        enriched.put("is_high_amount", isHighAmount);						// Add whether the transaction amount exceeds the high-amount threshold.

		// Convert event timestamp
        enriched.put("event_date", EnrichmentUtils.eventDate(transaction.eventTimestamp));		        		// Convert the event timestamp into a readable event date.
        enriched.put("event_hour", EnrichmentUtils.eventHour(transaction.eventTimestamp));						// Extract the hour from the transaction event timestamp.
		enriched.put("timestamp", java.time.Instant.ofEpochMilli(transaction.eventTimestamp).toString()); // Convert the event timestamp from milliseconds into an ISO-8601 timestamp string.

		// Record the current processing time when the transaction is enriched.
        enriched.put("processing_time", java.time.Instant.now().toString());

		// Update the customer's last data
        state.lastDeviceId = transaction.deviceId;							// Update the customer's last known device with the current transaction device.
        state.lastCity = transaction.city;									// Update the customer's last known city with the current transaction city.
        state.lastTransactionTimestamp = transaction.eventTimestamp;		// Update the customer's last transaction timestamp.
        state.lastAmount = transaction.amount;								// Update the customer's last transaction amount.

        // Persist the updated customer state so it can be used by the next transaction for this customer.
        customerState.update(state);

        // Serialize the enriched JSON object into a string and emit it to the downstream Kafka sink.
        out.collect(MAPPER.writeValueAsString(enriched));
    }
}