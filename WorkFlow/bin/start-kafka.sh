#!/bin/sh

ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/start-kafka.yml
