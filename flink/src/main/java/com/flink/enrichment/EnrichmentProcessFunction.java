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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final double HIGH_AMOUNT_THRESHOLD = Double.parseDouble(
	    System.getenv().getOrDefault("HIGH_AMOUNT_THRESHOLD", "100000000")
    );

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
    
    @Override
	public void open(OpenContext openContext) {
		ValueStateDescriptor<CustomerState> descriptor =
				new ValueStateDescriptor<>(
						"customer-state",
						Types.POJO(CustomerState.class)
				);

		customerState = getRuntimeContext().getState(descriptor);
	}

    @Override
    public void processElement(
	    RawTransaction transaction,
	    Context context,
	    Collector<String> out
    ) throws Exception {

	CustomerState state = customerState.value();

	if (state == null) {
	    state = new CustomerState();
	}

	boolean isNewDevice = EnrichmentUtils.isNewDevice(
		state.lastDeviceId,
		transaction.deviceId
	);

	boolean isNewLocation = EnrichmentUtils.isNewLocation(
		state.lastCity,
		transaction.city
	);

	boolean isNightTransaction = EnrichmentUtils.isNightTransaction(
		transaction.eventTimestamp
	);

	boolean isForeignTransaction = EnrichmentUtils.isForeignTransaction(
		transaction.country
	);

	boolean isHighAmount = EnrichmentUtils.isHighAmount(
		transaction.amount,
		HIGH_AMOUNT_THRESHOLD
	);

	String merchantRiskLevel =
		EnrichmentUtils.determineMerchantRisk(transaction);

	int accountAgeDays =
		EnrichmentUtils.determineAccountAge(transaction.customerId);

	String customerRiskLevel =
		EnrichmentUtils.determineCustomerRisk(accountAgeDays);

	ObjectNode enriched = MAPPER.createObjectNode();

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
	enriched.put(
		"event_date",
		EnrichmentUtils.eventDate(transaction.eventTimestamp)
	);
	enriched.put(
		"event_hour",
		EnrichmentUtils.eventHour(transaction.eventTimestamp)
	);
	enriched.put(
		"timestamp",
		java.time.Instant.ofEpochMilli(
			transaction.eventTimestamp
		).toString()
	);
	enriched.put(
		"processing_time",
		java.time.Instant.now().toString()
	);

	state.lastDeviceId = transaction.deviceId;
	state.lastCity = transaction.city;
	state.lastTransactionTimestamp = transaction.eventTimestamp;
	state.lastAmount = transaction.amount;

	customerState.update(state);

	out.collect(
		MAPPER.writeValueAsString(enriched)
	);
    }
}