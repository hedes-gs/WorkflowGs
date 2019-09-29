#!/bin/sh
cd /opt/kafka/latest/bin
export KAFKA_OPTS=" -Djava.security.auth.login.config=/opt/kafka/latest/config/client_jaas.conf -javaagent:/opt/kafka/latest/monitor/jmx_prometheus_javaagent-0.12.0.jar=7000:/opt/kafka/latest/monitor/config_prometheus.yml"
./kafka-server-start.sh /opt/kafka/latest/config/server.properties