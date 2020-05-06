#!/bin/sh

PID=`ps axo user:20,pid,command | grep java | grep wf_nfs | grep java | grep -v grep | awk '{print $2}'`
echo kill $PID
while  ! test -z $PID
do
  echo kill $PID
  kill $PID
  sleep 1
  PID=`ps axo user:20,pid,command | grep java | grep wf_nfs | grep java | grep -v grep | awk '{print $2}'`
done