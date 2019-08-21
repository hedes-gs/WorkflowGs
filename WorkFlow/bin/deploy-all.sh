#!/bin/bash

cd ../../WorkflowHbase/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v
cd ../..//WorkFlowProcessAndPublishForRecord/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v
cd ../../WorkFlowComputeHashKey/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v
cd ../../WorkFlowDuplicatecheck/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v
cd ../../WorkFlowCopyFile/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v
cd ../../WorkFlowExtractImageInfo/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v
cd ../../WorkflowScan/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v