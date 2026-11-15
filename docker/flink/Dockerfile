# Flink with the Kafka SQL connector, Iceberg, and the Hadoop libraries Iceberg needs.
FROM flink:2.0
ARG MAVEN=https://repo1.maven.org/maven2
# Kafka connector (Module 18)
ADD --chmod=644 ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/
# Iceberg for Flink 2.0 (Module 20); same Iceberg version as Project 1
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.1/iceberg-flink-runtime-2.0-1.10.1.jar /opt/flink/lib/
# Iceberg's file catalog uses Hadoop's file-system code, so Flink needs these on its classpath
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar /opt/flink/lib/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar /opt/flink/lib/
ADD --chmod=644 ${MAVEN}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar /opt/flink/lib/