#!/bin/sh
export HADOOP_HOME=/home/hadoop/latest
$HADOOP_HOME/bin/yarn --daemon start resourcemanager
