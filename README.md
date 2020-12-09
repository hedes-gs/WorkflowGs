# WorkflowGs

This project describes the way to archive and quickly retrieve your RAW sony images with some big data technologies :
- kafka, kafka stream
- hadoop, hbase
- apache storm

Currently, it is validated on a cluster of 6 machines like https://www.fit-pc.com/web/products/ipc2.

Ansible scripts are used to deploy the binaries and to update the versions of kafka, hbase, hadoop and storm in the cluster.
