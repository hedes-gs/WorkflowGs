#!/bin/bash
. /home/hbase/.bashrc
cd /home/hbase/latest/bin
./hbase-daemon.sh "$@"
