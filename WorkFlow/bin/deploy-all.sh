#!/bin/bash
CURRENT_PWD=$PWD

 cd $CURRENT_PWD/../../WorkFlowStorms;                                       ansible-playbook -i ../WorkFlow/ansible/inventory.yml ./ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkflowHbase/deployment;  						ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkFlowComputeHashKey/deployment;  				ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkFlowDuplicatecheck/deployment;  				ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v &
 cd $CURRENT_PWD/../../WorkFlowCopyFile/deployment;  						ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkFlowExtractImageInfo/deployment;  				ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkflowScan/deployment;  							ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkFlowArchiveFile/deployment;  					ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkflowProcessAndPublishExifData/deployment;  	ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkFlowProcessAndPublishThumbImages/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v &
# d $CURRENT_PWD/../../WorkFlowMonitor/deployment;  					    ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml -v  &
