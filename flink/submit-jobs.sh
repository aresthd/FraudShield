#!/bin/bash

set -e

FLINK_ENTRYPOINT="/docker-entrypoint.sh"
JOBMANAGER_HOST="jobmanager"
JOBMANAGER_PORT="8081"

TRANSACTION_JOB_NAME="TransactionEnrichment"
FRAUD_ENGINE_JOB_NAME="StatefulFraudEngine"

TRANSACTION_MAIN_CLASS="com.flink.enrichment.TransactionEnrichment"
FRAUD_ENGINE_MAIN_CLASS="com.flink.engine.StatefulFraudEngine"

TRANSACTION_JAR="/opt/flink/jobs/transaction-enrichment.jar"
FRAUD_ENGINE_JAR="/opt/flink/jobs/stateful-fraud-engine.jar"

TRANSACTION_CONFIG="/opt/flink/config/transaction-enrichment.properties"
FRAUD_ENGINE_CONFIG="/opt/flink/config/stateful-fraud-engine.properties"


wait_for_jobmanager() {
    echo "Waiting for Flink JobManager..."
    until curl -sf "http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}/overview" > /dev/null; do
        echo "JobManager is not ready yet..."
        sleep 5
    done
    echo "JobManager is ready."
}


wait_for_taskmanager() {
    echo "Waiting for TaskManager..."
    until curl -sf "http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}/taskmanagers" \
        | grep -q '"id"'; do
        echo "TaskManager is not registered yet..."
        sleep 5
    done
    echo "TaskManager is registered."
}


submit_job() {
    local job_name="$1"
    local main_class="$2"
    local jar_file="$3"
    local config_file="$4"
    echo ""
    echo "Checking job: ${job_name}"
    if flink list \
        -r \
        -m "${JOBMANAGER_HOST}:${JOBMANAGER_PORT}" 2>/dev/null \
        | grep -Fq "${job_name}"; then
        echo "Job '${job_name}' is already running."
        echo "Skipping submission."
        return 0
    fi
    echo "Submitting job: ${job_name}..."
    flink run -d \
        -m "${JOBMANAGER_HOST}:${JOBMANAGER_PORT}" \
        -c "${main_class}" \
        "${jar_file}" \
        "${config_file}" \
        "${job_name}"
    echo "Job '${job_name}' submitted successfully."
}


start_jobmanager() {
    echo "Starting FraudShield Flink JobManager..."
    # Start the original Flink JobManager entrypoint
    "${FLINK_ENTRYPOINT}" jobmanager &
    FLINK_PID=$!

    # Wait until JobManager REST API is available
    wait_for_jobmanager

    # Wait until at least one TaskManager is registered
    wait_for_taskmanager

    echo ""
    echo "Submitting FraudShield Flink Jobs..."
    submit_job \
        "${TRANSACTION_JOB_NAME}" \
        "${TRANSACTION_MAIN_CLASS}" \
        "${TRANSACTION_JAR}" \
        "${TRANSACTION_CONFIG}"

    submit_job \
        "${FRAUD_ENGINE_JOB_NAME}" \
        "${FRAUD_ENGINE_MAIN_CLASS}" \
        "${FRAUD_ENGINE_JAR}" \
        "${FRAUD_ENGINE_CONFIG}"

    echo ""
    echo "=========================================="
    echo "Running Flink Jobs"
    echo "=========================================="
    flink list \
        -r \
        -m "${JOBMANAGER_HOST}:${JOBMANAGER_PORT}" \
        2>/dev/null

    echo ""
    echo "FraudShield Flink JobManager is running."

    # Keep container alive and propagate Flink exit status
    wait "${FLINK_PID}"
}


start_taskmanager() {
    echo "Starting FraudShield Flink TaskManager..."
    exec "${FLINK_ENTRYPOINT}" taskmanager
}


case "$1" in
    jobmanager)
        start_jobmanager
        ;;

    taskmanager)
        start_taskmanager
        ;;

    *)
        echo "Executing original Flink entrypoint..."
        exec "${FLINK_ENTRYPOINT}" "$@"
        ;;

esac