#!/bin/sh

cd /home/wf_nfs 
/usr/lib/jvm/jdk-13.0.2+8/bin/java -Dlog4j.configurationFile=./config/log4j2.xml -Dlog4j2.debug=true -jar ./lib/WorkFlowNFSServer.jar
