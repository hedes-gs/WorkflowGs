#!/bin/bash

ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/disable-appli-as-services.yml -v
