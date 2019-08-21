#!/bin/sh

ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/stop-kafka-and-zookeeper.yml
