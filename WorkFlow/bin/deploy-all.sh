#!/bin/bash
CURRENT_PWD=$PWD
mkdir deploy

 cd $CURRENT_PWD/../../WorkFlowStorms;                                       ansible-playbook -i ../WorkFlow/ansible/inventory.yml ./ansible/deploy.yml -v  &
 cd $CURRENT_PWD/../../WorkflowHbase/deployment;  						ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml   	--limit 'wf_hbase' 							| tee $CURRENT_PWD/deploy/wf_hbase_deploy.txt &
  cd $CURRENT_PWD/../../WorkFlowComputeHashKey/deployment;  				ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml  	--limit 'wf_cmp_hashkey'    				| tee $CURRENT_PWD/deploy/wf_cmp_hashkey_deploy.txt &
  cd $CURRENT_PWD/../../WorkFlowDuplicatecheck/deployment;  				ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml  	--limit 'wf_dupcheck' 						| tee $CURRENT_PWD/deploy/wf_dupcheck_deploy.txt &
  cd $CURRENT_PWD/../../WorkFlowCopyFile/deployment;  						ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml --limit 'wf_copy_files'  					| tee $CURRENT_PWD/deploy/wf_copy_files_deploy.txt &
  cd $CURRENT_PWD/../../WorkFlowExtractImageInfo/deployment;  				ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml --limit 'wf_ext_img_infos'  				| tee $CURRENT_PWD/deploy/wf_ext_img_infos_deploy.txt &
  cd $CURRENT_PWD/../../WorkflowScan/deployment;  							ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml --limit 'wf_scan'   						| tee $CURRENT_PWD/deploy/wf_scan_deploy.txt &
  cd $CURRENT_PWD/../../WorkFlowArchiveFile/deployment;  					ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml --limit 'wf_archive'   						| tee $CURRENT_PWD/deploy/wf_archive_deploy.txt &
 cd $CURRENT_PWD/../../WorkflowProcessAndPublishExifData/deployment;  	ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml     --limit 'wf_proc_and_publish_exif_data'   	| tee $CURRENT_PWD/deploy/wf_proc_and_publish_exif_deploy.txt &
 cd $CURRENT_PWD/../../WorkFlowProcessAndPublishThumbImages/deployment;  ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml    --limit 'wf_proc_and_publish_photo'  		| tee $CURRENT_PWD/deploy/wf_proc_and_publish_photo_deploy.txt &
#  cd $CURRENT_PWD/../../WorkFlowMonitor/deployment;  					    ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/deploy.yml --limit 'wf_monitor'  						| tee $CURRENT_PWD/deploy/wf_proc_and_publish_photo_deploy.txt   &

wait
echo "---- Checking for issues...."
find $CURRENT_PWD/deploy -name '*.txt' -exec egrep -i 'failed=[1-9]+' {} \; -print

echo "---- End of checking for issues...."
