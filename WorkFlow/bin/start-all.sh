#!/bin/bash

cd ../../WorkFlowComputeHashKey/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/start.yml -v
cd ../../WorkFlowDuplicatecheck/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/start.yml -v
cd ../../WorkFlowCopyFile/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/start.yml -v
cd ../../WorkFlowExtractImageInfo/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/start.yml -v