#!/bin/bash

#Install python sudo apt install python-is-python3
ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/install-python.yml

# Install kerberos client
# you have also to run  sudo dpkg-reconfigure krb5-config on the new machines
ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml --limit ipc8,ipc7  ../../WorkFlow/ansible/install-kerberos-client.yml

# disable ipv6
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml  ../../WorkFlow/ansible/disable-ipv6.yml

# Mount disk , only for ipc7, ipc8 because we try to mount /dev/sdb
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml --limit ipc8,ipc7 ../../WorkFlow/ansible/mount-all.yml

# install java
# ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/install-java.yml

# create all users
# ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-all-users.yml

# create nfs
# ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/install-nfs.yml

# install ignite
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-all-ignite.yml

# install storm
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-kerberos-storm.yml
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-all-storm.yml

# install kafka
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-kerberos-kafka.yml
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-all-kafka.yml

# install hadoop
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-kerberos-hadoop.yml
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-all-hadoop.yml

# install hbase
ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml ../../WorkFlow/ansible/create-kerberos-hbase.yml
#ansible-playbook -i  ../../WorkFlow/ansible/inventory.yml  ../../WorkFlow/ansible/create-all-hbase.yml



#install applications
#./deploy-all.sh