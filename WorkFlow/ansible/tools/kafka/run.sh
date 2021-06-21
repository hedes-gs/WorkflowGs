#!/bin/sh
export JAVA_HOME=/usr/lib/jvm/jdk-11
cd /opt/kafka/latest/bin
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=65 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
export KAFKA_HEAP_OPTS="-Xmx1500m -Xms768m"

export KAFKA_OPTS=" -Djava.security.auth.login.config=/opt/kafka/latest/config/client_jaas.conf -javaagent:/opt/kafka/latest/monitor/jmx_prometheus_javaagent-0.12.0.jar=7000:/opt/kafka/latest/monitor/config_prometheus.yml"
./kafka-server-start.sh /opt/kafka/latest/config/server.properties