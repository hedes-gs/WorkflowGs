package com.gs.photo.workflow.archive.config;

public class SpecificApplicationProperties {

    protected int    batchSizeForParallelProcessingIncomingRecords;
    protected String zookeeperHosts;
    protected int    zookeeperPort;
    protected String hadoopPrincipal;
    protected String hadoopKeyTab;
    protected String rootPath;
    protected String nameSpace;
    protected String producerName;

    public String getProducerName() { return this.producerName; }

    public void setProducerName(String producerName) { this.producerName = producerName; }

    public String getNameSpace() { return this.nameSpace; }

    public void setNameSpace(String nameSpace) { this.nameSpace = nameSpace; }

    public int getBatchSizeForParallelProcessingIncomingRecords() {
        return this.batchSizeForParallelProcessingIncomingRecords;
    }

    public void setBatchSizeForParallelProcessingIncomingRecords(int batchSizeForParallelProcessingIncomingRecords) {
        this.batchSizeForParallelProcessingIncomingRecords = batchSizeForParallelProcessingIncomingRecords;
    }

    public String getZookeeperHosts() { return this.zookeeperHosts; }

    public void setZookeeperHosts(String zookeeperHosts) { this.zookeeperHosts = zookeeperHosts; }

    public int getZookeeperPort() { return this.zookeeperPort; }

    public void setZookeeperPort(int zookeeperPort) { this.zookeeperPort = zookeeperPort; }

    public String getHadoopPrincipal() { return this.hadoopPrincipal; }

    public void setHadoopPrincipal(String hadoopPrincipal) { this.hadoopPrincipal = hadoopPrincipal; }

    public String getHadoopKeyTab() { return this.hadoopKeyTab; }

    public void setHadoopKeyTab(String hadoopKeyTab) { this.hadoopKeyTab = hadoopKeyTab; }

    public String getRootPath() { return this.rootPath; }

    public void setRootPath(String rootPath) { this.rootPath = rootPath; }

}
