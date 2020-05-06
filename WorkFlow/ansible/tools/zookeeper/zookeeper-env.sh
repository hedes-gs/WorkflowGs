SERVER_JVMFLAGS=-Dzookeeper.DigestAuthenticationProvider.superDigest=super:xQJmxLMiHGwaqBvst5y6rkB6HQs=
ZOOMAIN=org.apache.zookeeper.server.quorum.QuorumPeerMain
ZOOCFGDIR=/home/zookeeper/latest/conf
ZOOCFG=zoo.cfg
ZOO_LOG_DIR=/home/zookeeper/latest/log
ZOO_LOG4J_PROP=DEBUG,ROLLINGFILE,CONSOLE
JMXLOCALONLY=true
JVMFLAGS=" -Xmx512m -Djava.security.auth.login.config=/home/zookeeper/latest/conf/client_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.krb5.debug=true -Dzookeeper.kerberos.removeHostFromPrincipal=true -Dzookeeper.kerberos.removeRealmFromPrincipal=true"
# If ZooKeeper is started through systemd, this will only be used for command
# line tools such as `zkCli.sh` and not for the actual server
JAVA=/usr/lib/jvm/java-1.8.0-openjdk-amd64/bin/java
# TODO: This is really ugly
# How to find out which jars are needed?
# Seems that log4j requires the log4j.properties file to be in the classpath
ZOO_DATADIR=/home/zookeeper/latest