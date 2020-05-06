#!/bin/bash
set -x
cd /home/hbase
. /home/hbase/.bashrc
/home/hbase/latest/bin/hbase-daemon.sh stop master
/home/hbase/latest/bin/regionservers.sh /home/hbase/latest/bin/hbase-daemon.sh stop regionserver