FROM flink:2.3.0
# RUN mkdir -p /opt/flink/usrlib
# RUN wget -P /opt/flink/usrlib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar

USER root

RUN mkdir -p /opt/flink/jobs /opt/flink/config /opt/flink/scripts

COPY target/transaction-enrichment.jar /opt/flink/jobs/transaction-enrichment.jar
COPY target/stateful-fraud-engine.jar /opt/flink/jobs/stateful-fraud-engine.jar
COPY config/ /opt/flink/config/

COPY submit-jobs.sh /opt/flink/scripts/submit-jobs.sh
RUN chmod +x /opt/flink/scripts/submit-jobs.sh

USER flink

ENTRYPOINT ["/opt/flink/scripts/submit-jobs.sh"]

