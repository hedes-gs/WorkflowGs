#!/bin/bash
cd ..
cd bin

./stop-all.sh
./stop-kafka-zookeeper.sh
./purge-k-z.sh
./deploy-all.sh
./start-kafka-zookeeper.sh
./start-all.sh
