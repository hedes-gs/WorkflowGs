package com.gs.photo.workflow.recinhbase.consumers.config;

public class SpecificApplicationProperties {

    protected int    batchSizeForParallelProcessingIncomingRecords;
    protected String zookeeperHosts;
    protected int    zookeeperPort;
    protected String hbasePrincipal;
    protected String hbaseKeyTable;
    protected String nameSpace;
    protected String producerName;

    public String getProducerName() { return this.producerName; }

    public void setProducerName(String producerName) { this.producerName = producerName; }

    public String getNameSpace() { return this.nameSpace; }

    public void setNameSpace(String nameSpace) { this.nameSpace = nameSpace; }

    public String getHbasePrincipal() { return this.hbasePrincipal; }

    public void setHbasePrincipal(String hbasePrincipal) { this.hbasePrincipal = hbasePrincipal; }

    public String getHbaseKeyTable() { return this.hbaseKeyTable; }

    public void setHbaseKeyTable(String hbaseKeyTable) { this.hbaseKeyTable = hbaseKeyTable; }

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

}
