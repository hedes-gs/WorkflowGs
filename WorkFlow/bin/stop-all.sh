#!/bin/bash

ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/stop-appli-as-services.yml -v
