#!/bin/bash

ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/start-appli-as-services.yml -v
