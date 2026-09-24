FROM confluentinc/cp-kafka-connect-base:7.5.0

USER root

# Download the JMX Prometheus javaagent
RUN mkdir -p /opt/jmx_exporter && \
    curl -sL https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar \
    -o /opt/jmx_exporter/jmx_prometheus_javaagent.jar

# Copy the JMX exporter config
COPY kafka-connect.yml /opt/jmx_exporter/kafka-connect.yml

USER appuser

# Install Debezium PostgreSQL Connector using confluent-hub
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4-3
