#!/bin/sh

cd /home/wf_nfs/bin 
/usr/lib/jvm/jdk-13.0.2+8/bin/java -Dlog4j.configuration=../config/log4j.xml -jar ../lib/WorkFlowNFSServer.jar
