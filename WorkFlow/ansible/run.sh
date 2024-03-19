#!/bin/sh
cd /opt/kafka/latest/bin
export KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/latest/config/client_jass.conf"
./kafka-server-start.sh /opt/kafka/latest/config/server.properties