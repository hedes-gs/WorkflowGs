#!/bin/bash

cd ..
export MAVEN_HOME=/mnt/e/vms/maven/apache-maven-3.5.3
${MAVEN_HOME}/bin/mvn clean install spring-boot:repackage
cd ansible
# ansible-playbook -i inventory.yml  deploy.yml